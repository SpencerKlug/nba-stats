"""NBA stats agent: natural-language questions → SQL → answers.

Uses an LLM to translate plain-English questions into DuckDB SQL,
executes the query, and summarises the results conversationally.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import textwrap

import duckdb
from openai import OpenAI

log = logging.getLogger(__name__)

_SAMPLE_ROWS = 3

_SYSTEM_PROMPT = textwrap.dedent("""\
You are an expert NBA data analyst with access to a DuckDB database.
Given a user's question about NBA statistics, generate a SQL query to answer it.

{schema}

{samples}

Instructions:
- Write DuckDB-compatible SQL.
- Prefer the marts tables (main_marts.*) for summary / analytics questions.
- Use staging tables (main_staging.*) when the user asks for raw game-level detail.
- Respond with ONLY a valid JSON object: {{"sql": "<query>", "explanation": "<one-liner>"}}
- Do NOT wrap the JSON in markdown code fences.
- If the question cannot be answered from the available data, set "sql" to null
  and explain why in "explanation".
""")

_ANSWER_SYSTEM = (
    "You are a helpful NBA stats analyst. "
    "Summarise query results clearly and conversationally, using specific numbers. "
    "Format rankings or lists with bullet points."
)

_ANSWER_USER = textwrap.dedent("""\
The user asked: {question}

SQL executed:
{sql}

Results:
{results}

Provide a clear, concise answer in natural language.
""")


class NBAStatsAgent:
    """Interactive agent that answers NBA stats questions via text-to-SQL."""

    def __init__(
        self,
        db_path: str = "warehouse.duckdb",
        model: str = "gpt-4o-mini",
    ) -> None:
        if not os.environ.get("OPENAI_API_KEY"):
            print(
                "Error: OPENAI_API_KEY environment variable is not set.\n"
                "Set it with:  export OPENAI_API_KEY='sk-...'"
            )
            sys.exit(1)

        self.con = duckdb.connect(db_path, read_only=True)
        self.client = OpenAI()
        self.model = model
        self._system_prompt = self._build_system_prompt()

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def _build_system_prompt(self) -> str:
        """Introspect DuckDB and build a schema + samples prompt for the LLM."""
        tables = self.con.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """
        ).fetchall()

        schema_lines: list[str] = []
        sample_lines: list[str] = []

        for schema, table in tables:
            fq = f"{schema}.{table}"
            cols = self.con.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
                "ORDER BY ordinal_position"
            ).fetchall()
            col_str = ", ".join(f"{c} {t}" for c, t in cols)
            schema_lines.append(f"  {fq}({col_str})")

            if schema in ("main_marts", "main_staging"):
                try:
                    sample = self.con.execute(f"SELECT * FROM {fq} LIMIT {_SAMPLE_ROWS}").fetchdf()
                    if not sample.empty:
                        sample_lines.append(f"-- {fq} (sample):\n{sample.to_string(index=False)}")
                except Exception:
                    pass

        schema_text = "Database tables:\n" + "\n".join(schema_lines)
        samples_text = "Sample data:\n\n" + "\n\n".join(sample_lines) if sample_lines else ""
        return _SYSTEM_PROMPT.format(schema=schema_text, samples=samples_text)

    # ------------------------------------------------------------------
    # LLM helpers
    # ------------------------------------------------------------------

    def _chat(self, system: str, user: str) -> str:
        """Send a single chat-completion request and return the content."""
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0,
        )
        return resp.choices[0].message.content.strip()

    def _generate_sql(self, question: str) -> dict | None:
        """Ask the LLM to produce a SQL query for *question*.

        Returns a dict with ``sql`` and ``explanation`` keys, or ``None``
        if the response cannot be parsed.
        """
        raw = self._chat(self._system_prompt, question)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("LLM response was not valid JSON: %s", raw)
            return None

    def _summarise(self, question: str, sql: str, results: str) -> str:
        """Ask the LLM to summarise *results* as a natural-language answer."""
        user_msg = _ANSWER_USER.format(question=question, sql=sql, results=results)
        return self._chat(_ANSWER_SYSTEM, user_msg)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ask(self, question: str) -> str | None:
        """Answer a natural-language question about NBA stats.

        Prints the generated SQL, the query results, and a conversational
        answer.  Returns the answer string, or ``None`` on failure.
        """
        parsed = self._generate_sql(question)
        if not parsed or not parsed.get("sql"):
            msg = (
                parsed.get("explanation", "Could not generate a query.")
                if parsed
                else "Sorry, I couldn't understand that question."
            )
            print(f"\n  {msg}")
            return None

        sql: str = parsed["sql"]
        print(f"\n  SQL: {sql}")
        if parsed.get("explanation"):
            print(f"  ({parsed['explanation']})")

        # Execute — with one retry on failure
        try:
            result_df = self.con.execute(sql).fetchdf()
        except Exception as exc:
            retry = self._generate_sql(
                f"{question}\n\nThe previous SQL failed with: {exc}\nPlease fix."
            )
            if not retry or not retry.get("sql"):
                print(f"\n  Query error: {exc}")
                return None
            sql = retry["sql"]
            print(f"  Retried SQL: {sql}")
            try:
                result_df = self.con.execute(sql).fetchdf()
            except Exception as exc2:
                print(f"\n  Query error: {exc2}")
                return None

        if result_df.empty:
            print("\n  No results found.")
            return None

        results_str = result_df.to_string(index=False)
        answer = self._summarise(question, sql, results_str)
        print(f"\n  {answer}")
        return answer

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self.con.close()
