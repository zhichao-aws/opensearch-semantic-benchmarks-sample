import os
from aiohttp import web
from tokenizer_wrapper import TokenizerWrapper

TOKENIZER = TokenizerWrapper(tokenizer_path="tokenizer.json", idf_path="idf.json")

async def ping(request: web.Request) -> web.Response:
    # SageMaker uses /ping for health checks
    return web.Response(text="ok", status=200)


def _normalize_inputs(inputs):
    if isinstance(inputs, str):
        return [inputs]
    if isinstance(inputs, list) and all(isinstance(x, str) for x in inputs):
        return inputs

    raise web.HTTPBadRequest(text="inputs must be a string or a list of strings")


async def invocations(request: web.Request) -> web.Response:
    texts = _normalize_inputs(await request.json())
    encoded = [TOKENIZER.encode(text) for text in texts]
    
    return web.json_response(encoded, status=200)


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ping", ping)
    app.router.add_post("/invocations", invocations)
    return app


# Gunicorn will import "app:app"
app = create_app()

if __name__ == "__main__":
    # Local debug mode (single-process)
    port = int(os.getenv("PORT", "8080"))
    web.run_app(app, host="0.0.0.0", port=port)
