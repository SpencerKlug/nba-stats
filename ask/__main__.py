"""CLI entry-point: ``python3 -m ask``

Usage:
    python3 -m ask "Who leads the league in scoring?"
    python3 -m ask                        # interactive REPL
    python3 -m ask --model gpt-4o "..."   # use a different model
"""

from __future__ import annotations

import argparse
import sys

from ask.agent import NBAStatsAgent


def main() -> int:
    parser = argparse.ArgumentParser(description="Ask natural-language questions about NBA stats")
    parser.add_argument(
        "question",
        nargs="?",
        default=None,
        help="Question to ask (omit for interactive mode)",
    )
    parser.add_argument("--db", default="warehouse.duckdb", help="Path to DuckDB file")
    parser.add_argument(
        "--model", default="gpt-4o-mini", help="OpenAI model (default: gpt-4o-mini)"
    )
    args = parser.parse_args()

    agent = NBAStatsAgent(db_path=args.db, model=args.model)

    if args.question:
        agent.ask(args.question)
    else:
        print("NBA Stats Agent — ask me anything about NBA player stats.")
        print("Type 'quit' or 'exit' to leave.\n")
        while True:
            try:
                q = input("You: ").strip()
                if not q:
                    continue
                if q.lower() in ("quit", "exit", "q"):
                    break
                agent.ask(q)
                print()
            except (KeyboardInterrupt, EOFError):
                print()
                break

    agent.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
