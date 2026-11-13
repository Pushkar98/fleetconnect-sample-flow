"""Mock AI agent — the agentic pattern without a real LLM.

The shape is exactly the tool-use loop of an LLM API (Claude, etc.):

    while True:
        decision = model(question, tools, history)       # <- the ONLY mocked part
        if decision is "end_turn":  return answer
        if decision is "tool_use":  result = call_tool(...)  -> append to history, loop

`mock_model()` below is a few if/else rules. In production you replace it with one call:
    client.messages.create(model=..., tools=TOOLS, messages=history)
and nothing else changes: same TOOLS, same tool executor, same guardrails.

Architecture rules this demonstrates:
  * The agent never touches PostGIS or Kafka. Its tools are PUBLIC API ENDPOINTS, so it gets
    the same validation, privacy rules and audit trail as every other channel.
  * Every tool call is recorded in the returned trace (auditability).
  * A step limit stops runaway loops.
  * The answer is composed ONLY from tool results (grounding: no invented restaurants).

Run from the CLI:  docker compose exec api python agent/agent.py "restaurants from Part-Dieu to the airport"
"""
import json
import os
import re
import sys
import unicodedata

import httpx

API_URL = os.getenv("API_URL", "http://localhost:8000")
MAX_STEPS = 6

# Tool definitions, in the exact JSON-schema format an LLM tool-use API expects.
TOOLS = [
    {"name": "find_place", "endpoint": "GET /places?q=",
     "description": "Resolve a place name in the Lyon area to lon/lat coordinates.",
     "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
    {"name": "plan_route", "endpoint": "GET /route?from=&to=",
     "description": "Driving route between two named places, returned as a GeoJSON LineString.",
     "input_schema": {"type": "object", "properties": {"from": {"type": "string"}, "to": {"type": "string"}},
                      "required": ["from", "to"]}},
    {"name": "find_restaurants_along_route", "endpoint": "POST /restaurants/along-route",
     "description": "Recommended restaurants within buffer_m metres of a route LineString.",
     "input_schema": {"type": "object", "properties": {"route": {"type": "object"}, "buffer_m": {"type": "integer"}},
                      "required": ["route"]}},
    {"name": "find_nearest_dealers", "endpoint": "GET /dealers/nearest",
     "description": "Nearest tire dealers to a point, optionally only those stocking tire_size.",
     "input_schema": {"type": "object", "properties": {"lon": {"type": "number"}, "lat": {"type": "number"},
                                                       "tire_size": {"type": "string"}}, "required": ["lon", "lat"]}},
]


# ------------------------------------------------------------ tool executor
def call_tool(client: httpx.Client, name: str, args: dict) -> dict:
    """Map a tool name to an HTTP call on the public API. The agent's only way to see data."""
    if name == "find_place":
        r = client.get("/places", params={"q": args["name"]})
    elif name == "plan_route":
        r = client.get("/route", params={"from": args["from"], "to": args["to"]})
    elif name == "find_restaurants_along_route":
        r = client.post("/restaurants/along-route", json={"route": args["route"], "buffer_m": args.get("buffer_m", 1000), "limit": 100})
    elif name == "find_nearest_dealers":
        r = client.get("/dealers/nearest", params={k: v for k, v in args.items() if v is not None} | {"limit": 3})
    else:
        return {"error": f"unknown tool {name}"}
    return r.json() if r.status_code < 400 else {"error": r.json().get("detail", r.text)}


# --------------------------------------------------------------- mock model
def _norm(t: str) -> str:
    return unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode().lower()


def tool_use(name, args, thought):
    return {"stop_reason": "tool_use", "name": name, "input": args, "thought": thought}


def end_turn(text):
    return {"stop_reason": "end_turn", "text": text}


def mock_model(question: str, history: list[dict]) -> dict:
    """Decide the next step from the question + tool results so far. Rules, not an LLM."""
    q = _norm(question)
    done = {h["tool"]: h["result"] for h in history}
    if history and "error" in history[-1]["result"]:
        return end_turn(f"Sorry, I couldn't complete that: {history[-1]['result']['error']}")

    # Intent 1: restaurants along a route ("... from X to Y")
    if any(w in q for w in ["restaurant", "eat", "lunch", "dinner", "food"]):
        m = re.search(r"from (.+?) to (.+?)(?:[?.!,]|$| and )", q)
        if not m:
            return end_turn("Tell me where you start and where you're going, e.g. 'from Part-Dieu to the airport'.")
        if "plan_route" not in done:
            return tool_use("plan_route", {"from": m.group(1), "to": m.group(2)},
                            "I need the route geometry before I can search along it.")
        if "find_restaurants_along_route" not in done:
            return tool_use("find_restaurants_along_route", {"route": done["plan_route"]["geometry"], "buffer_m": 1000},
                            "Now search a 1 km corridor around that route.")
        route, found = done["plan_route"]["properties"], done["find_restaurants_along_route"]["features"]
        if not found:
            return end_turn(f"I found no recommended restaurants within 1 km of {route['from']} -> {route['to']}.")
        best = sorted(found, key=lambda f: -f["properties"]["recommendation_score"])[:3]
        lines = [f"{i}. {f['properties']['name']} - {f['properties']['cuisine']}, score {f['properties']['recommendation_score']}, "
                 f"{f['properties']['price_band']}, {int(f['properties']['metres_off_route'])} m off route"
                 for i, f in enumerate(best, 1)]
        return end_turn(f"On your {route['distance_m'] / 1000:.1f} km drive from {route['from']} to {route['to']} "
                        f"I found {len(found)} restaurants within 1 km. Top rated:\n" + "\n".join(lines))

    # Intent 2: nearest dealer ("dealer near X", optional tire size like 205/55R16)
    if any(w in q for w in ["dealer", "tyre", "tire", "garage"]):
        m = re.search(r"(?:near|around|in|at|close to) ([a-z' -]+?)(?: that| with| for| stocking|[?.!,]|$)", q)
        size = re.search(r"\d{3}/\d{2}\s?r\s?\d{2}(?:\.\d)?", q)
        size = size.group(0).upper().replace(" ", "") if size else None
        if not m:
            return end_turn("Where are you? e.g. 'nearest dealer near Bron with 205/55R16'.")
        if "find_place" not in done:
            return tool_use("find_place", {"name": m.group(1)}, "First turn the place name into coordinates.")
        if "find_nearest_dealers" not in done:
            p = done["find_place"]
            return tool_use("find_nearest_dealers", {"lon": p["lon"], "lat": p["lat"], "tire_size": size},
                            "Now find the nearest dealers" + (f" that stock {size}." if size else "."))
        found = done["find_nearest_dealers"]["features"]
        if not found:
            return end_turn(f"No dealer near {done['find_place']['name']} stocks {size}.")
        lines = [f"{i}. {f['properties']['name']} - {f['properties']['distance_m'] / 1000:.1f} km, "
                 f"{f['properties']['address']} ({f['properties']['opening_hours']})" for i, f in enumerate(found, 1)]
        return end_turn(f"Nearest dealers to {done['find_place']['name']}" + (f" stocking {size}" if size else "") +
                        ":\n" + "\n".join(lines))

    return end_turn("I can find (a) restaurants along a drive, e.g. 'restaurants from Vieux Lyon to Neuville', "
                    "or (b) tire dealers, e.g. 'nearest dealer near Bron with 205/55R16'.")


# ---------------------------------------------------------------- agent loop
def _summarise(obj):
    """Keep the trace readable: don't print 13-point geometries."""
    if isinstance(obj, dict) and obj.get("type") == "LineString":
        return f"<LineString, {len(obj['coordinates'])} points>"
    if isinstance(obj, dict) and obj.get("type") == "FeatureCollection":
        return f"<{len(obj['features'])} features>"
    if isinstance(obj, dict) and obj.get("type") == "Feature":
        return {"geometry": _summarise(obj["geometry"]), **obj["properties"]}
    if isinstance(obj, dict):
        return {k: _summarise(v) for k, v in obj.items() if k != "corridor"}
    return obj


def run_agent(question: str, api_url: str = API_URL) -> dict:
    trace, history = [{"type": "user", "text": question}], []
    with httpx.Client(base_url=api_url, timeout=15) as client:
        for _ in range(MAX_STEPS):
            d = mock_model(question, history)
            if d["stop_reason"] == "end_turn":
                trace.append({"type": "answer", "text": d["text"]})
                break
            trace.append({"type": "thought", "text": d["thought"]})
            trace.append({"type": "tool_call", "tool": d["name"], "input": _summarise(d["input"]),
                          "endpoint": next(t["endpoint"] for t in TOOLS if t["name"] == d["name"])})
            result = call_tool(client, d["name"], d["input"])
            history.append({"tool": d["name"], "result": result})
            trace.append({"type": "tool_result", "tool": d["name"], "output": _summarise(result)})
        else:
            trace.append({"type": "answer", "text": "Stopped: step limit reached."})
    # hand the raw geometries to the map so the answer can be drawn
    return {"trace": trace, "map": {h["tool"]: h["result"] for h in history}}


if __name__ == "__main__":
    out = run_agent(" ".join(sys.argv[1:]) or "restaurants from Part-Dieu to the airport")
    for step in out["trace"]:
        kind = step["type"].upper().ljust(11)
        body = step.get("text") or json.dumps({k: v for k, v in step.items() if k != "type"}, ensure_ascii=False)
        print(f"{kind} {body}\n")
