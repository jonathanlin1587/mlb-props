"""CLI entrypoint shim. Implementation lives in ``app.tracker``."""

from __future__ import annotations

from app.tracker import main

if __name__ == "__main__":
    raise SystemExit(main())
