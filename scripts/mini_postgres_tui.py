#!/usr/bin/env python3
"""Mini database lab in Python with a CLI TUI.

This educational implementation supports three toy engines:
- Postgres-inspired row-store with WAL, MVCC, and scan planning
- DynamoDB-inspired key-value store with partition keys and eventual GSIs
- VectorDB-inspired embedding store with ANN-style partition search

The goal is conceptual fidelity, not feature parity with production systems.
"""

from __future__ import annotations

import argparse
import csv
import curses
import datetime as dt
import json
import math
import os
import re
import shlex
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


SUPPORTED_TYPES = {"INT", "FLOAT", "TEXT", "BOOL"}


class MiniPgError(Exception):
    """Base error for user-facing database failures."""


class SQLParseError(MiniPgError):
    """Raised when the SQL parser cannot parse a statement."""


class MiniDynamoError(MiniPgError):
    """Raised when the DynamoDB-inspired engine hits an execution error."""


class MiniVectorError(MiniPgError):
    """Raised when the vector engine hits an execution error."""


@dataclass
class ExecResult:
    message: str
    rows: Optional[List[Dict[str, Any]]] = None
    plan: Optional[Dict[str, Any]] = None


@dataclass
class Transaction:
    xid: int
    snapshot_xid: int
    pending_ops: List[Dict[str, Any]] = field(default_factory=list)


class SQLParser:
    _IDENT = r"[A-Za-z_][A-Za-z0-9_]*"

    def parse(self, sql: str) -> Dict[str, Any]:
        text = sql.strip()
        if not text:
            raise SQLParseError("Empty statement")
        if text.endswith(";"):
            text = text[:-1].strip()

        upper = text.upper()
        if upper == "BEGIN":
            return {"type": "begin"}
        if upper == "COMMIT":
            return {"type": "commit"}
        if upper == "ROLLBACK":
            return {"type": "rollback"}
        if upper in {"QUIT", "EXIT", "\\Q"}:
            return {"type": "quit"}
        if upper == "HELP":
            return {"type": "help"}
        if upper == "SHOW TABLES":
            return {"type": "show_tables"}
        if upper == "CHECKPOINT":
            return {"type": "checkpoint"}
        if upper.startswith("VACUUM"):
            return self._parse_vacuum(text)
        if upper.startswith("DESCRIBE"):
            return self._parse_describe(text)
        if upper.startswith("EXPLAIN"):
            return self._parse_explain(text)
        if upper.startswith("CREATE TABLE"):
            return self._parse_create_table(text)
        if upper.startswith("CREATE INDEX"):
            return self._parse_create_index(text)
        if upper.startswith("INSERT"):
            return self._parse_insert(text)
        if upper.startswith("SELECT"):
            return self._parse_select(text)
        if upper.startswith("UPDATE"):
            return self._parse_update(text)
        if upper.startswith("DELETE"):
            return self._parse_delete(text)

        raise SQLParseError(f"Unsupported statement: {text}")

    def _parse_vacuum(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^VACUUM(?:\s+(?P<table>" + self._IDENT + r"))?$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid VACUUM syntax")
        return {"type": "vacuum", "table": m.group("table")}

    def _parse_describe(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^DESCRIBE\s+(?P<table>" + self._IDENT + r")$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid DESCRIBE syntax")
        return {"type": "describe", "table": m.group("table")}

    def _parse_explain(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^EXPLAIN\s+(.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid EXPLAIN syntax")
        inner = m.group(1).strip()
        if inner.upper().startswith("EXPLAIN"):
            raise SQLParseError("Nested EXPLAIN is not supported")
        return {"type": "explain", "statement": self.parse(inner)}

    def _parse_create_table(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^CREATE\s+TABLE\s+(?P<table>" + self._IDENT + r")\s*\((?P<cols>.+)\)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid CREATE TABLE syntax")

        columns: List[Dict[str, str]] = []
        for raw_col in self._split_csv(m.group("cols")):
            parts = raw_col.strip().split()
            if len(parts) != 2:
                raise SQLParseError(f"Invalid column definition: {raw_col}")
            name, col_type = parts[0], parts[1].upper()
            if not re.match(r"^" + self._IDENT + r"$", name):
                raise SQLParseError(f"Invalid column name: {name}")
            if col_type not in SUPPORTED_TYPES:
                raise SQLParseError(
                    f"Unsupported type {col_type}. Supported types: {', '.join(sorted(SUPPORTED_TYPES))}"
                )
            columns.append({"name": name, "type": col_type})

        if not columns:
            raise SQLParseError("Table must have at least one column")
        return {"type": "create_table", "table": m.group("table"), "columns": columns}

    def _parse_create_index(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^CREATE\s+INDEX\s+(?P<index>" + self._IDENT + r")\s+ON\s+(?P<table>"
            + self._IDENT
            + r")\s*\((?P<column>"
            + self._IDENT
            + r")\)$",
            text,
            re.IGNORECASE,
        )
        if not m:
            raise SQLParseError("Invalid CREATE INDEX syntax")
        return {
            "type": "create_index",
            "index": m.group("index"),
            "table": m.group("table"),
            "column": m.group("column"),
        }

    def _parse_insert(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^INSERT\s+INTO\s+(?P<table>"
            + self._IDENT
            + r")\s*(?:\((?P<cols>[^)]*)\))?\s+VALUES\s*\((?P<values>.*)\)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid INSERT syntax")

        columns = None
        if m.group("cols"):
            columns = [name.strip() for name in self._split_csv(m.group("cols"))]
            if not all(columns):
                raise SQLParseError("Invalid INSERT column list")

        values = [self._parse_literal(v.strip()) for v in self._split_csv(m.group("values"))]
        return {"type": "insert", "table": m.group("table"), "columns": columns, "values": values}

    def _parse_select(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^SELECT\s+(?P<cols>.+?)\s+FROM\s+(?P<table>"
            + self._IDENT
            + r")(?:\s+WHERE\s+(?P<where>.+))?$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid SELECT syntax")

        cols_text = m.group("cols").strip()
        if cols_text == "*":
            columns = ["*"]
        else:
            columns = [c.strip() for c in self._split_csv(cols_text)]
            if not columns:
                raise SQLParseError("SELECT requires columns or *")

        where = self._parse_where(m.group("where"))
        return {"type": "select", "table": m.group("table"), "columns": columns, "where": where}

    def _parse_update(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^UPDATE\s+(?P<table>" + self._IDENT + r")\s+SET\s+(?P<set>.+?)(?:\s+WHERE\s+(?P<where>.+))?$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid UPDATE syntax")

        assignments: Dict[str, Any] = {}
        for piece in self._split_csv(m.group("set")):
            am = re.match(r"^(" + self._IDENT + r")\s*=\s*(.+)$", piece.strip(), re.IGNORECASE | re.DOTALL)
            if not am:
                raise SQLParseError(f"Invalid assignment: {piece}")
            assignments[am.group(1)] = self._parse_literal(am.group(2).strip())

        where = self._parse_where(m.group("where"))
        return {"type": "update", "table": m.group("table"), "set": assignments, "where": where}

    def _parse_delete(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^DELETE\s+FROM\s+(?P<table>" + self._IDENT + r")(?:\s+WHERE\s+(?P<where>.+))?$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid DELETE syntax")
        where = self._parse_where(m.group("where"))
        return {"type": "delete", "table": m.group("table"), "where": where}

    def _parse_where(self, where_text: Optional[str]) -> List[Dict[str, Any]]:
        if where_text is None:
            return []

        clauses = re.split(r"\s+AND\s+", where_text.strip(), flags=re.IGNORECASE)
        result: List[Dict[str, Any]] = []
        for clause in clauses:
            m = re.match(r"^(" + self._IDENT + r")\s*(=|!=|>=|<=|>|<)\s*(.+)$", clause.strip(), re.IGNORECASE)
            if not m:
                raise SQLParseError(f"Unsupported WHERE clause: {clause}")
            result.append(
                {
                    "column": m.group(1),
                    "op": m.group(2),
                    "value": self._parse_literal(m.group(3).strip()),
                }
            )
        return result

    @staticmethod
    def _split_csv(blob: str) -> List[str]:
        reader = csv.reader([blob], skipinitialspace=True, quotechar="'", escapechar="\\")
        return next(reader)

    @staticmethod
    def _parse_literal(token: str) -> Any:
        t = token.strip()
        if not t:
            return ""

        upper = t.upper()
        if upper == "NULL":
            return None
        if upper == "TRUE":
            return True
        if upper == "FALSE":
            return False

        if (t.startswith("'") and t.endswith("'")) or (t.startswith('"') and t.endswith('"')):
            return t[1:-1]

        if re.match(r"^-?\d+$", t):
            return int(t)
        if re.match(r"^-?\d+\.\d+$", t):
            return float(t)
        return t


class DynamoCommandParser:
    _IDENT = r"[A-Za-z_][A-Za-z0-9_]*"

    def parse(self, command: str) -> Dict[str, Any]:
        text = command.strip()
        if not text:
            raise SQLParseError("Empty command")
        if text.endswith(";"):
            text = text[:-1].strip()

        upper = text.upper()
        if upper in {"QUIT", "EXIT", "\\Q"}:
            return {"type": "quit"}
        if upper == "HELP":
            return {"type": "help"}
        if upper == "SHOW TABLES":
            return {"type": "show_tables"}
        if upper.startswith("DESCRIBE"):
            return self._parse_describe(text)
        if upper.startswith("EXPLAIN"):
            return self._parse_explain(text)
        if upper.startswith("CREATE TABLE"):
            return self._parse_create_table(text)
        if upper.startswith("CREATE GSI"):
            return self._parse_create_gsi(text)
        if upper.startswith("PUT "):
            return self._parse_put(text)
        if upper.startswith("GET "):
            return self._parse_get(text)
        if upper.startswith("QUERY GSI") or upper.startswith("QUERY INDEX"):
            return self._parse_query_gsi(text)
        if upper.startswith("QUERY "):
            return self._parse_query(text)
        if upper.startswith("SCAN "):
            return self._parse_scan(text)
        if upper.startswith("UPDATE "):
            return self._parse_update(text)
        if upper.startswith("DELETE "):
            return self._parse_delete(text)
        if upper.startswith("TICK"):
            return self._parse_tick(text)

        raise SQLParseError(f"Unsupported Dynamo command: {text}")

    def _parse_describe(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^DESCRIBE\s+(?P<table>" + self._IDENT + r")$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid DESCRIBE syntax")
        return {"type": "describe", "table": m.group("table")}

    def _parse_explain(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^EXPLAIN\s+(.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid EXPLAIN syntax")
        inner = m.group(1).strip()
        if inner.upper().startswith("EXPLAIN"):
            raise SQLParseError("Nested EXPLAIN is not supported")
        stmt = self.parse(inner)
        if stmt["type"] in {"help", "show_tables", "describe", "tick", "quit"}:
            raise SQLParseError("EXPLAIN supports GET/QUERY/SCAN/QUERY GSI only")
        return {"type": "explain", "statement": stmt}

    def _parse_create_table(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^CREATE\s+TABLE\s+(?P<table>" + self._IDENT + r")\s+PK\s+(?P<pk>" + self._IDENT + r")"
            r"(?:\s+SK\s+(?P<sk>" + self._IDENT + r"))?"
            r"(?:\s+RCU\s+(?P<rcu>\d+))?"
            r"(?:\s+WCU\s+(?P<wcu>\d+))?$",
            text,
            re.IGNORECASE,
        )
        if not m:
            raise SQLParseError("Invalid CREATE TABLE syntax. Example: CREATE TABLE orders PK user_id SK order_id")
        return {
            "type": "create_table",
            "table": m.group("table"),
            "pk": m.group("pk"),
            "sk": m.group("sk"),
            "rcu": int(m.group("rcu")) if m.group("rcu") else 100,
            "wcu": int(m.group("wcu")) if m.group("wcu") else 100,
        }

    def _parse_create_gsi(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^CREATE\s+GSI\s+(?P<index>" + self._IDENT + r")\s+ON\s+(?P<table>" + self._IDENT + r")\s*"
            r"\((?P<attribute>" + self._IDENT + r")\)"
            r"(?:\s+PROJECT\s+(?P<projection>ALL|KEYS_ONLY))?$",
            text,
            re.IGNORECASE,
        )
        if not m:
            raise SQLParseError("Invalid CREATE GSI syntax. Example: CREATE GSI gsi_email ON users (email)")
        return {
            "type": "create_gsi",
            "index": m.group("index"),
            "table": m.group("table"),
            "attribute": m.group("attribute"),
            "projection": (m.group("projection") or "ALL").upper(),
        }

    def _parse_put(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^PUT\s+(?P<table>" + self._IDENT + r")\s+(?P<body>.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid PUT syntax")
        attrs = self._parse_assignments(m.group("body"))
        if not attrs:
            raise SQLParseError("PUT requires at least one key/value assignment")
        return {"type": "put", "table": m.group("table"), "item": attrs}

    def _parse_get(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^GET\s+(?P<table>" + self._IDENT + r")\s+(?P<body>.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid GET syntax")

        key: Dict[str, Any] = {}
        consistent = False
        tokens = shlex.split(m.group("body"))
        for token in tokens:
            upper = token.upper()
            if upper == "CONSISTENT":
                consistent = True
                continue
            if upper == "EVENTUAL":
                consistent = False
                continue
            if "=" not in token:
                raise SQLParseError(f"Invalid GET token: {token}")
            name, value = token.split("=", 1)
            self._validate_identifier(name)
            key[name] = self._parse_literal(value)

        if not key:
            raise SQLParseError("GET requires key assignments")
        return {"type": "get", "table": m.group("table"), "key": key, "consistent": consistent}

    def _parse_query(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^QUERY\s+(?P<table>" + self._IDENT + r")\s+(?P<body>.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid QUERY syntax")

        key_cond: Optional[Dict[str, Any]] = None
        sk_begins = None
        limit = None
        consistent = False
        tokens = shlex.split(m.group("body"))

        i = 0
        while i < len(tokens):
            token = tokens[i]
            upper = token.upper()
            if upper == "SK_BEGINS":
                if i + 1 >= len(tokens):
                    raise SQLParseError("SK_BEGINS requires a value")
                sk_begins = self._parse_literal(tokens[i + 1])
                i += 2
                continue
            if upper == "LIMIT":
                if i + 1 >= len(tokens):
                    raise SQLParseError("LIMIT requires a number")
                try:
                    limit = int(tokens[i + 1])
                except ValueError as exc:
                    raise SQLParseError("LIMIT must be an integer") from exc
                if limit <= 0:
                    raise SQLParseError("LIMIT must be positive")
                i += 2
                continue
            if upper == "CONSISTENT":
                consistent = True
                i += 1
                continue
            if upper == "EVENTUAL":
                consistent = False
                i += 1
                continue
            if "=" in token:
                if key_cond is not None:
                    raise SQLParseError("QUERY supports one partition-key equality condition")
                name, value = token.split("=", 1)
                self._validate_identifier(name)
                key_cond = {"column": name, "value": self._parse_literal(value)}
                i += 1
                continue
            raise SQLParseError(f"Unrecognized QUERY token: {token}")

        if key_cond is None:
            raise SQLParseError("QUERY requires partition-key equality. Example: QUERY orders user_id=42")
        return {
            "type": "query",
            "table": m.group("table"),
            "key_cond": key_cond,
            "sk_begins": sk_begins,
            "limit": limit,
            "consistent": consistent,
        }

    def _parse_query_gsi(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^QUERY\s+(?:GSI|INDEX)\s+(?P<index>" + self._IDENT + r")\s+(?P<body>.+)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid QUERY GSI syntax")

        attr_cond: Optional[Dict[str, Any]] = None
        limit = None
        tokens = shlex.split(m.group("body"))
        i = 0
        while i < len(tokens):
            token = tokens[i]
            upper = token.upper()
            if upper == "LIMIT":
                if i + 1 >= len(tokens):
                    raise SQLParseError("LIMIT requires a number")
                try:
                    limit = int(tokens[i + 1])
                except ValueError as exc:
                    raise SQLParseError("LIMIT must be an integer") from exc
                if limit <= 0:
                    raise SQLParseError("LIMIT must be positive")
                i += 2
                continue
            if "=" in token:
                if attr_cond is not None:
                    raise SQLParseError("QUERY GSI supports one equality condition")
                name, value = token.split("=", 1)
                self._validate_identifier(name)
                attr_cond = {"column": name, "value": self._parse_literal(value)}
                i += 1
                continue
            raise SQLParseError(f"Unrecognized QUERY GSI token: {token}")

        if attr_cond is None:
            raise SQLParseError("QUERY GSI requires an attribute equality condition")
        return {"type": "query_gsi", "index": m.group("index"), "attr_cond": attr_cond, "limit": limit}

    def _parse_scan(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^SCAN\s+(?P<table>" + self._IDENT + r")"
            r"(?:\s+WHERE\s+(?P<where>.+?))?"
            r"(?:\s+LIMIT\s+(?P<limit>\d+))?$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid SCAN syntax")
        where = self._parse_where(m.group("where"))
        limit = int(m.group("limit")) if m.group("limit") else None
        return {"type": "scan", "table": m.group("table"), "where": where, "limit": limit}

    def _parse_update(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^UPDATE\s+(?P<table>" + self._IDENT + r")\s+(?P<keys>.+?)\s+SET\s+(?P<set>.+)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid UPDATE syntax")
        key = self._parse_assignments(m.group("keys"))
        set_values = self._parse_assignments(m.group("set"))
        if not key:
            raise SQLParseError("UPDATE requires key assignments before SET")
        if not set_values:
            raise SQLParseError("UPDATE requires at least one SET assignment")
        return {"type": "update", "table": m.group("table"), "key": key, "set": set_values}

    def _parse_delete(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^DELETE\s+(?P<table>" + self._IDENT + r")\s+(?P<body>.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid DELETE syntax")
        key = self._parse_assignments(m.group("body"))
        if not key:
            raise SQLParseError("DELETE requires key assignments")
        return {"type": "delete", "table": m.group("table"), "key": key}

    def _parse_tick(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^TICK(?:\s+(?P<count>\d+))?$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid TICK syntax")
        count = int(m.group("count")) if m.group("count") else 1
        if count <= 0:
            raise SQLParseError("TICK count must be positive")
        return {"type": "tick", "count": count}

    def _parse_where(self, where_text: Optional[str]) -> List[Dict[str, Any]]:
        if not where_text:
            return []

        parts = re.split(r"\s+AND\s+", where_text.strip(), flags=re.IGNORECASE)
        clauses: List[Dict[str, Any]] = []
        for part in parts:
            m = re.match(r"^(" + self._IDENT + r")\s*(=|!=|>=|<=|>|<)\s*(.+)$", part.strip(), re.IGNORECASE)
            if not m:
                raise SQLParseError(f"Unsupported SCAN WHERE clause: {part}")
            clauses.append(
                {
                    "column": m.group(1),
                    "op": m.group(2),
                    "value": self._parse_literal(m.group(3).strip()),
                }
            )
        return clauses

    def _parse_assignments(self, text: str) -> Dict[str, Any]:
        normalized = text.replace(",", " ")
        tokens = shlex.split(normalized)
        result: Dict[str, Any] = {}
        for token in tokens:
            if "=" not in token:
                raise SQLParseError(f"Expected assignment key=value, got: {token}")
            name, value = token.split("=", 1)
            self._validate_identifier(name)
            result[name] = self._parse_literal(value)
        return result

    def _validate_identifier(self, identifier: str) -> None:
        if not re.match(r"^" + self._IDENT + r"$", identifier):
            raise SQLParseError(f"Invalid identifier: {identifier}")

    @staticmethod
    def _parse_literal(token: str) -> Any:
        t = token.strip()
        if not t:
            return ""
        upper = t.upper()
        if upper == "NULL":
            return None
        if upper == "TRUE":
            return True
        if upper == "FALSE":
            return False
        if re.match(r"^-?\d+$", t):
            return int(t)
        if re.match(r"^-?\d+\.\d+$", t):
            return float(t)
        return t


class VectorCommandParser:
    _IDENT = r"[A-Za-z_][A-Za-z0-9_]*"

    def parse(self, command: str) -> Dict[str, Any]:
        text = command.strip()
        if not text:
            raise SQLParseError("Empty command")
        if text.endswith(";"):
            text = text[:-1].strip()

        upper = text.upper()
        if upper in {"QUIT", "EXIT", "\\Q"}:
            return {"type": "quit"}
        if upper == "HELP":
            return {"type": "help"}
        if upper in {"SHOW COLLECTIONS", "SHOW TABLES"}:
            return {"type": "show_collections"}
        if upper.startswith("DESCRIBE"):
            return self._parse_describe(text)
        if upper.startswith("EXPLAIN"):
            return self._parse_explain(text)
        if upper.startswith("CREATE COLLECTION"):
            return self._parse_create_collection(text)
        if upper.startswith("DROP COLLECTION"):
            return self._parse_drop_collection(text)
        if upper.startswith("UPSERT"):
            return self._parse_upsert(text)
        if upper.startswith("GET"):
            return self._parse_get(text)
        if upper.startswith("DELETE"):
            return self._parse_delete(text)
        if upper.startswith("SEARCH"):
            return self._parse_search(text)
        if upper.startswith("LIST"):
            return self._parse_list(text)
        if upper.startswith("REBUILD INDEX"):
            return self._parse_rebuild_index(text)
        if upper.startswith("TICK"):
            return self._parse_tick(text)

        raise SQLParseError(f"Unsupported Vector command: {text}")

    def _parse_describe(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^DESCRIBE\s+(?P<collection>" + self._IDENT + r")$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid DESCRIBE syntax")
        return {"type": "describe", "collection": m.group("collection")}

    def _parse_explain(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^EXPLAIN\s+(.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid EXPLAIN syntax")
        inner = m.group(1).strip()
        if inner.upper().startswith("EXPLAIN"):
            raise SQLParseError("Nested EXPLAIN is not supported")
        stmt = self.parse(inner)
        if stmt["type"] not in {"search", "get", "list"}:
            raise SQLParseError("EXPLAIN supports SEARCH/GET/LIST only")
        return {"type": "explain", "statement": stmt}

    def _parse_create_collection(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^CREATE\s+COLLECTION\s+(?P<collection>" + self._IDENT + r")\s+DIM\s+(?P<dim>\d+)"
            r"(?:\s+METRIC\s+(?P<metric>COSINE|L2|DOT))?"
            r"(?:\s+PARTITIONS\s+(?P<partitions>\d+))?$",
            text,
            re.IGNORECASE,
        )
        if not m:
            raise SQLParseError(
                "Invalid CREATE COLLECTION syntax. Example: CREATE COLLECTION docs DIM 4 METRIC COSINE PARTITIONS 8"
            )
        dim = int(m.group("dim"))
        if dim <= 0:
            raise SQLParseError("DIM must be positive")
        partitions = int(m.group("partitions")) if m.group("partitions") else 8
        if partitions <= 0:
            raise SQLParseError("PARTITIONS must be positive")
        return {
            "type": "create_collection",
            "collection": m.group("collection"),
            "dim": dim,
            "metric": (m.group("metric") or "COSINE").lower(),
            "partitions": partitions,
        }

    def _parse_drop_collection(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^DROP\s+COLLECTION\s+(?P<collection>" + self._IDENT + r")$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid DROP COLLECTION syntax")
        return {"type": "drop_collection", "collection": m.group("collection")}

    def _parse_upsert(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^UPSERT\s+(?P<collection>" + self._IDENT + r")\s+(?P<body>.+)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid UPSERT syntax")

        item_id: Optional[str] = None
        vector: Optional[List[float]] = None
        metadata: Dict[str, Any] = {}
        for token in shlex.split(m.group("body")):
            if "=" not in token:
                raise SQLParseError(f"Expected assignment key=value, got: {token}")
            key, value = token.split("=", 1)
            self._validate_identifier(key)
            lower = key.lower()
            if lower == "id":
                item_id = str(self._parse_literal(value))
            elif lower == "vector":
                vector = self._parse_vector(value)
            else:
                metadata[key] = self._parse_literal(value)

        if item_id is None:
            raise SQLParseError("UPSERT requires id=<value>")
        if vector is None:
            raise SQLParseError("UPSERT requires vector=[...]")
        return {
            "type": "upsert",
            "collection": m.group("collection"),
            "id": item_id,
            "vector": vector,
            "metadata": metadata,
        }

    def _parse_get(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^GET\s+(?P<collection>" + self._IDENT + r")\s+(?P<body>.+)$", text, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLParseError("Invalid GET syntax")
        assignments = self._parse_assignments_tokens(shlex.split(m.group("body")))
        if set(assignments.keys()) != {"id"}:
            raise SQLParseError("GET expects only id=<value>")
        return {"type": "get", "collection": m.group("collection"), "id": str(assignments["id"])}

    def _parse_delete(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^DELETE\s+(?P<collection>" + self._IDENT + r")\s+(?P<body>.+)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid DELETE syntax")
        assignments = self._parse_assignments_tokens(shlex.split(m.group("body")))
        if set(assignments.keys()) != {"id"}:
            raise SQLParseError("DELETE expects only id=<value>")
        return {"type": "delete", "collection": m.group("collection"), "id": str(assignments["id"])}

    def _parse_search(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^SEARCH\s+(?P<collection>" + self._IDENT + r")\s+(?P<body>.+)$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            raise SQLParseError("Invalid SEARCH syntax")

        vector: Optional[List[float]] = None
        topk = 5
        probe = 2
        exact = False
        filters: Dict[str, Any] = {}
        tokens = shlex.split(m.group("body"))

        i = 0
        while i < len(tokens):
            token = tokens[i]
            upper = token.upper()
            if upper == "TOPK":
                if i + 1 >= len(tokens):
                    raise SQLParseError("TOPK requires a number")
                try:
                    topk = int(tokens[i + 1])
                except ValueError as exc:
                    raise SQLParseError("TOPK must be an integer") from exc
                i += 2
                continue
            if upper == "PROBE":
                if i + 1 >= len(tokens):
                    raise SQLParseError("PROBE requires a number")
                try:
                    probe = int(tokens[i + 1])
                except ValueError as exc:
                    raise SQLParseError("PROBE must be an integer") from exc
                i += 2
                continue
            if upper == "EXACT":
                exact = True
                i += 1
                continue
            if upper == "FILTER":
                filters = self._parse_assignments_tokens(tokens[i + 1 :])
                break
            if "=" in token:
                key, value = token.split("=", 1)
                self._validate_identifier(key)
                if key.lower() != "vector":
                    raise SQLParseError("Only vector=<...> is allowed before FILTER")
                vector = self._parse_vector(value)
                i += 1
                continue
            raise SQLParseError(f"Unrecognized SEARCH token: {token}")

        if vector is None:
            raise SQLParseError("SEARCH requires vector=[...]")
        if topk <= 0:
            raise SQLParseError("TOPK must be positive")
        if probe <= 0:
            raise SQLParseError("PROBE must be positive")

        return {
            "type": "search",
            "collection": m.group("collection"),
            "vector": vector,
            "topk": topk,
            "probe": probe,
            "exact": exact,
            "filters": filters,
        }

    def _parse_list(self, text: str) -> Dict[str, Any]:
        m = re.match(
            r"^LIST\s+(?P<collection>" + self._IDENT + r")(?:\s+LIMIT\s+(?P<limit>\d+))?$",
            text,
            re.IGNORECASE,
        )
        if not m:
            raise SQLParseError("Invalid LIST syntax. Example: LIST docs LIMIT 20")
        limit = int(m.group("limit")) if m.group("limit") else 20
        if limit <= 0:
            raise SQLParseError("LIMIT must be positive")
        return {"type": "list", "collection": m.group("collection"), "limit": limit}

    def _parse_rebuild_index(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^REBUILD\s+INDEX\s+(?P<collection>" + self._IDENT + r")$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid REBUILD INDEX syntax")
        return {"type": "rebuild_index", "collection": m.group("collection")}

    def _parse_tick(self, text: str) -> Dict[str, Any]:
        m = re.match(r"^TICK(?:\s+(?P<count>\d+))?$", text, re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid TICK syntax")
        count = int(m.group("count")) if m.group("count") else 1
        if count <= 0:
            raise SQLParseError("TICK count must be positive")
        return {"type": "tick", "count": count}

    def _parse_assignments_tokens(self, tokens: List[str]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for token in tokens:
            if "=" not in token:
                raise SQLParseError(f"Expected assignment key=value, got: {token}")
            key, value = token.split("=", 1)
            self._validate_identifier(key)
            if key.lower() == "vector":
                result[key] = self._parse_vector(value)
            else:
                result[key] = self._parse_literal(value)
        return result

    def _validate_identifier(self, identifier: str) -> None:
        if not re.match(r"^" + self._IDENT + r"$", identifier):
            raise SQLParseError(f"Invalid identifier: {identifier}")

    def _parse_vector(self, token: str) -> List[float]:
        t = token.strip()
        if not (t.startswith("[") and t.endswith("]")):
            raise SQLParseError("Vector literal must be bracketed, e.g. [0.1,0.2,0.3]")
        inner = t[1:-1].strip()
        if not inner:
            return []
        values: List[float] = []
        for chunk in inner.split(","):
            piece = chunk.strip()
            try:
                values.append(float(piece))
            except ValueError as exc:
                raise SQLParseError(f"Invalid vector element: {piece}") from exc
        return values

    @staticmethod
    def _parse_literal(token: str) -> Any:
        t = token.strip()
        if not t:
            return ""
        upper = t.upper()
        if upper == "NULL":
            return None
        if upper == "TRUE":
            return True
        if upper == "FALSE":
            return False
        if re.match(r"^-?\d+$", t):
            return int(t)
        if re.match(r"^-?\d+\.\d+$", t):
            return float(t)
        return t


class MiniPostgres:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.catalog_path = self.data_dir / "catalog.json"
        self.state_path = self.data_dir / "state.json"
        self.wal_path = self.data_dir / "wal.log"

        self.parser = SQLParser()
        self.active_tx: Optional[Transaction] = None

        self.catalog = self._load_catalog()
        self.state = self._load_state()
        self.next_lsn = self._load_next_lsn()

        self._recover_from_wal()

    def execute(self, sql: str) -> ExecResult:
        stmt = self.parser.parse(sql)
        stype = stmt["type"]

        if stype == "help":
            return ExecResult(self.help_text())

        if stype == "quit":
            return ExecResult("QUIT")

        if stype == "begin":
            return self.begin()

        if stype == "commit":
            return self.commit()

        if stype == "rollback":
            return self.rollback()

        if stype == "show_tables":
            return self.show_tables()

        if stype == "describe":
            return self.describe_table(stmt["table"])

        if stype == "checkpoint":
            return self.checkpoint()

        if stype == "vacuum":
            self._assert_not_in_tx("VACUUM")
            removed = self.vacuum(stmt["table"])
            return ExecResult(f"VACUUM removed {removed} obsolete row versions")

        if stype == "explain":
            return self.explain(stmt["statement"])

        if stype == "select":
            rows, plan = self._run_select(stmt)
            return ExecResult(f"SELECT returned {len(rows)} row(s)", rows=rows, plan=plan)

        if stype == "create_table":
            self._assert_not_in_tx("CREATE TABLE")
            op = self._build_create_table_op(stmt)
            self._autocommit_ops([op])
            return ExecResult(f"Table {stmt['table']} created")

        if stype == "create_index":
            self._assert_not_in_tx("CREATE INDEX")
            op = self._build_create_index_op(stmt)
            self._autocommit_ops([op])
            return ExecResult(f"Index {stmt['index']} created")

        if stype == "insert":
            op = self._build_insert_op(stmt)
            if self.active_tx:
                self.active_tx.pending_ops.append(op)
                return ExecResult(f"INSERT staged in tx {self.active_tx.xid}")
            affected = self._autocommit_ops([op])
            return ExecResult(f"INSERT {affected} row(s)")

        if stype == "update":
            op = self._build_update_op(stmt)
            if self.active_tx:
                self.active_tx.pending_ops.append(op)
                return ExecResult(f"UPDATE staged in tx {self.active_tx.xid}")
            affected = self._autocommit_ops([op])
            return ExecResult(f"UPDATE affected {affected} row(s)")

        if stype == "delete":
            op = self._build_delete_op(stmt)
            if self.active_tx:
                self.active_tx.pending_ops.append(op)
                return ExecResult(f"DELETE staged in tx {self.active_tx.xid}")
            affected = self._autocommit_ops([op])
            return ExecResult(f"DELETE affected {affected} row(s)")

        raise MiniPgError(f"Internal error: unhandled statement type {stype}")

    def begin(self) -> ExecResult:
        if self.active_tx:
            raise MiniPgError("Transaction already open")
        xid = self._next_xid()
        self.active_tx = Transaction(xid=xid, snapshot_xid=self.state["applied_xid"])
        return ExecResult(f"BEGIN tx={xid}, snapshot={self.active_tx.snapshot_xid}")

    def commit(self) -> ExecResult:
        if not self.active_tx:
            raise MiniPgError("No active transaction")

        tx = self.active_tx
        self._write_tx_wal(tx.xid, tx.pending_ops)
        affected = self._apply_ops(tx.pending_ops, tx.xid)
        self.state["applied_xid"] = tx.xid
        self._save_state()

        committed_ops = len(tx.pending_ops)
        self.active_tx = None
        return ExecResult(f"COMMIT tx={tx.xid} ({committed_ops} op(s), {affected} row mutation(s))")

    def rollback(self) -> ExecResult:
        if not self.active_tx:
            raise MiniPgError("No active transaction")
        txid = self.active_tx.xid
        staged = len(self.active_tx.pending_ops)
        self.active_tx = None
        return ExecResult(f"ROLLBACK tx={txid} ({staged} staged op(s) discarded)")

    def show_tables(self) -> ExecResult:
        rows = [{"table": t} for t in sorted(self.catalog["tables"].keys())]
        return ExecResult(f"{len(rows)} table(s)", rows=rows)

    def describe_table(self, table: str) -> ExecResult:
        schema = self._require_table(table)
        rows = []
        index_by_col = self._indexes_by_column(table)
        for col in schema["columns"]:
            rows.append(
                {
                    "column": col["name"],
                    "type": col["type"],
                    "indexed": ", ".join(index_by_col.get(col["name"], [])) or "-",
                }
            )
        return ExecResult(f"Schema for {table}", rows=rows)

    def explain(self, inner_stmt: Dict[str, Any]) -> ExecResult:
        if inner_stmt["type"] != "select":
            raise MiniPgError("EXPLAIN currently supports SELECT only")
        plan = self._plan_select(inner_stmt, include_pending=bool(self.active_tx))
        if plan["node"] == "Seq Scan":
            msg = f"Seq Scan on {plan['table']} (rows≈{plan['estimated_rows']}, filter={plan['predicate']})"
        else:
            msg = (
                f"Index Scan on {plan['table']} using {plan['index']} "
                f"(rows≈{plan['estimated_rows']}, filter={plan['predicate']})"
            )
        return ExecResult(msg, plan=plan)

    def checkpoint(self) -> ExecResult:
        self._append_wal(self.state["applied_xid"], "CHECKPOINT", {"applied_xid": self.state["applied_xid"]})
        return ExecResult(f"CHECKPOINT at xid={self.state['applied_xid']}")

    def vacuum(self, table: Optional[str]) -> int:
        targets = [table] if table else sorted(self.catalog["tables"].keys())
        removed = 0
        for tbl in targets:
            self._require_table(tbl)
            versions = self._load_versions(tbl)
            kept = []
            for v in versions:
                xmax = v.get("xmax")
                if xmax is not None and xmax <= self.state["applied_xid"]:
                    removed += 1
                    continue
                kept.append(v)
            self._save_versions(tbl, kept)
            self._rebuild_indexes_for_table(tbl)
        return removed

    def help_text(self) -> str:
        return "\n".join(
            [
                "Commands:",
                "  CREATE TABLE users (id INT, name TEXT, balance INT)",
                "  CREATE INDEX idx_users_id ON users (id)",
                "  INSERT INTO users VALUES (1, 'alice', 10)",
                "  SELECT * FROM users WHERE id = 1",
                "  UPDATE users SET balance = 20 WHERE id = 1",
                "  DELETE FROM users WHERE id = 1",
                "  BEGIN / COMMIT / ROLLBACK",
                "  EXPLAIN SELECT * FROM users WHERE id = 1",
                "  SHOW TABLES / DESCRIBE users / VACUUM [table] / CHECKPOINT",
                "  HELP / QUIT",
            ]
        )

    @staticmethod
    def model_name() -> str:
        return "Postgres (toy)"

    @staticmethod
    def prompt_tag() -> str:
        return "mini-pg"

    @staticmethod
    def concept_lines() -> List[str]:
        return [
            "Concepts:",
            "  WAL-first commits for crash safety.",
            "  MVCC row versions via xmin/xmax.",
            "  UPDATE is delete+insert versioning.",
            "  Planner chooses Seq Scan or Index Scan.",
        ]

    @staticmethod
    def example_commands_line() -> str:
        return "Examples: CREATE TABLE, INSERT, SELECT, EXPLAIN, BEGIN/COMMIT"

    def runtime_status_lines(self) -> List[str]:
        lines = [
            f"data dir     : {self.data_dir}",
            f"applied xid  : {self.state['applied_xid']}",
            f"next xid     : {self.state['next_xid']}",
            f"tables       : {len(self.catalog['tables'])}",
            f"indexes      : {len(self.catalog['indexes'])}",
        ]
        if self.active_tx:
            lines.append(f"active tx    : xid={self.active_tx.xid}, staged_ops={len(self.active_tx.pending_ops)}")
        else:
            lines.append("active tx    : none")

        for table in sorted(self.catalog["tables"].keys()):
            live = len(self._visible_rows(table, self.state["applied_xid"]))
            total = len(self._load_versions(table))
            lines.append(f"{table:<12}: live={live}, versions={total}")
        return lines

    def _assert_not_in_tx(self, verb: str) -> None:
        if self.active_tx:
            raise MiniPgError(f"{verb} is not supported inside an explicit transaction in this toy engine")

    def _run_select(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = stmt["table"]
        schema = self._require_table(table)
        self._validate_select_columns(schema, stmt["columns"])
        self._validate_where(schema, stmt["where"])

        if self.active_tx:
            rows = self._rows_with_pending_overlay(table, self.active_tx.snapshot_xid, self.active_tx.pending_ops)
            filtered = [row for row in rows if self._matches_where(row, stmt["where"])]
            projected = self._project_rows(filtered, stmt["columns"], schema)
            plan = {"node": "Seq Scan", "table": table, "predicate": self._where_text(stmt["where"]), "estimated_rows": len(rows)}
            return projected, plan

        plan = self._plan_select(stmt, include_pending=False)
        if plan["node"] == "Index Scan":
            idx_data = self._load_index_data(plan["index"])
            cond = stmt["where"][0]
            key = self._key_token(cond["value"])
            candidate_rids = idx_data.get("map", {}).get(key, [])

            visible = self._visible_rows(table, self.state["applied_xid"])
            by_rid = {row["rid"]: row["data"] for row in visible}
            filtered = []
            for rid in candidate_rids:
                data = by_rid.get(rid)
                if data is None:
                    continue
                if self._matches_where(data, stmt["where"]):
                    filtered.append(data)
        else:
            filtered = [
                row["data"]
                for row in self._visible_rows(table, self.state["applied_xid"])
                if self._matches_where(row["data"], stmt["where"])
            ]

        projected = self._project_rows(filtered, stmt["columns"], schema)
        return projected, plan

    def _plan_select(self, stmt: Dict[str, Any], include_pending: bool) -> Dict[str, Any]:
        table = stmt["table"]
        predicate = self._where_text(stmt["where"])
        if include_pending:
            est = len(self._rows_with_pending_overlay(table, self.active_tx.snapshot_xid, self.active_tx.pending_ops)) if self.active_tx else 0
            return {"node": "Seq Scan", "table": table, "predicate": predicate, "estimated_rows": est}

        if len(stmt["where"]) == 1 and stmt["where"][0]["op"] == "=":
            cond = stmt["where"][0]
            index_name = self._index_for_column(table, cond["column"])
            if index_name:
                idx_data = self._load_index_data(index_name)
                token = self._key_token(cond["value"])
                est = len(idx_data.get("map", {}).get(token, []))
                return {
                    "node": "Index Scan",
                    "table": table,
                    "index": index_name,
                    "predicate": predicate,
                    "estimated_rows": est,
                }

        est = len(self._visible_rows(table, self.state["applied_xid"]))
        return {"node": "Seq Scan", "table": table, "predicate": predicate, "estimated_rows": est}

    def _rows_with_pending_overlay(self, table: str, snapshot_xid: int, pending_ops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = [dict(row["data"]) for row in self._visible_rows(table, snapshot_xid)]
        for op in pending_ops:
            if op.get("table") != table:
                continue
            kind = op["kind"]
            if kind == "insert":
                rows.append(dict(op["values"]))
            elif kind == "update":
                for row in rows:
                    if self._matches_where(row, op["where"]):
                        row.update(op["set"])
            elif kind == "delete":
                rows = [row for row in rows if not self._matches_where(row, op["where"])]
        return rows

    def _build_create_table_op(self, stmt: Dict[str, Any]) -> Dict[str, Any]:
        table = stmt["table"]
        if table in self.catalog["tables"]:
            raise MiniPgError(f"Table already exists: {table}")

        seen = set()
        for col in stmt["columns"]:
            if col["name"] in seen:
                raise MiniPgError(f"Duplicate column name: {col['name']}")
            seen.add(col["name"])
        return {"kind": "create_table", "table": table, "columns": stmt["columns"]}

    def _build_create_index_op(self, stmt: Dict[str, Any]) -> Dict[str, Any]:
        index = stmt["index"]
        table = stmt["table"]
        column = stmt["column"]
        schema = self._require_table(table)
        if index in self.catalog["indexes"]:
            raise MiniPgError(f"Index already exists: {index}")
        self._require_column(schema, column)
        return {"kind": "create_index", "index": index, "table": table, "column": column}

    def _build_insert_op(self, stmt: Dict[str, Any]) -> Dict[str, Any]:
        schema = self._require_table(stmt["table"])
        columns_def = schema["columns"]
        schema_cols = [c["name"] for c in columns_def]

        if stmt["columns"] is None:
            cols = schema_cols
        else:
            cols = stmt["columns"]
            for c in cols:
                self._require_column(schema, c)

        if len(cols) != len(stmt["values"]):
            raise MiniPgError("INSERT columns and values count mismatch")

        row: Dict[str, Any] = {col: None for col in schema_cols}
        type_by_col = {c["name"]: c["type"] for c in columns_def}
        for col, value in zip(cols, stmt["values"]):
            row[col] = self._coerce(value, type_by_col[col])

        return {"kind": "insert", "table": stmt["table"], "values": row}

    def _build_update_op(self, stmt: Dict[str, Any]) -> Dict[str, Any]:
        schema = self._require_table(stmt["table"])
        self._validate_where(schema, stmt["where"])
        type_by_col = {c["name"]: c["type"] for c in schema["columns"]}
        set_values: Dict[str, Any] = {}
        for col, value in stmt["set"].items():
            self._require_column(schema, col)
            set_values[col] = self._coerce(value, type_by_col[col])
        return {"kind": "update", "table": stmt["table"], "set": set_values, "where": stmt["where"]}

    def _build_delete_op(self, stmt: Dict[str, Any]) -> Dict[str, Any]:
        schema = self._require_table(stmt["table"])
        self._validate_where(schema, stmt["where"])
        return {"kind": "delete", "table": stmt["table"], "where": stmt["where"]}

    def _autocommit_ops(self, ops: List[Dict[str, Any]]) -> int:
        xid = self._next_xid()
        self._write_tx_wal(xid, ops)
        affected = self._apply_ops(ops, xid)
        self.state["applied_xid"] = xid
        self._save_state()
        return affected

    def _apply_ops(self, ops: List[Dict[str, Any]], xid: int) -> int:
        total_affected = 0
        touched_tables = set()
        for op in ops:
            kind = op["kind"]
            if kind == "create_table":
                self.catalog["tables"][op["table"]] = {"columns": op["columns"], "next_rid": 1}
                self._table_path(op["table"]).touch(exist_ok=True)
                self._save_catalog()
                touched_tables.add(op["table"])
            elif kind == "create_index":
                self.catalog["indexes"][op["index"]] = {
                    "table": op["table"],
                    "column": op["column"],
                }
                self._save_catalog()
                self._rebuild_index(op["index"], snapshot_xid=xid)
            elif kind == "insert":
                total_affected += self._apply_insert(op, xid)
                touched_tables.add(op["table"])
            elif kind == "update":
                total_affected += self._apply_update(op, xid)
                touched_tables.add(op["table"])
            elif kind == "delete":
                total_affected += self._apply_delete(op, xid)
                touched_tables.add(op["table"])
            else:
                raise MiniPgError(f"Unknown operation kind: {kind}")

        for table in touched_tables:
            self._rebuild_indexes_for_table(table, snapshot_xid=xid)
        return total_affected

    def _apply_insert(self, op: Dict[str, Any], xid: int) -> int:
        table = op["table"]
        versions = self._load_versions(table)
        rid = self._allocate_rid(table)
        versions.append({"rid": rid, "xmin": xid, "xmax": None, "data": op["values"]})
        self._save_versions(table, versions)
        return 1

    def _apply_update(self, op: Dict[str, Any], xid: int) -> int:
        table = op["table"]
        versions = self._load_versions(table)
        updated = 0
        base_snapshot = self.state["applied_xid"]

        indexes = []
        for idx, version in enumerate(versions):
            if self._visible_for_apply(version, base_snapshot, xid) and self._matches_where(version["data"], op["where"]):
                indexes.append(idx)

        for idx in indexes:
            old = versions[idx]
            old["xmax"] = xid
            rid = self._allocate_rid(table)
            new_data = dict(old["data"])
            new_data.update(op["set"])
            versions.append({"rid": rid, "xmin": xid, "xmax": None, "data": new_data})
            updated += 1

        self._save_versions(table, versions)
        return updated

    def _apply_delete(self, op: Dict[str, Any], xid: int) -> int:
        table = op["table"]
        versions = self._load_versions(table)
        deleted = 0
        base_snapshot = self.state["applied_xid"]

        for version in versions:
            if self._visible_for_apply(version, base_snapshot, xid) and self._matches_where(version["data"], op["where"]):
                version["xmax"] = xid
                deleted += 1

        self._save_versions(table, versions)
        return deleted

    def _write_tx_wal(self, xid: int, ops: List[Dict[str, Any]]) -> None:
        self._append_wal(xid, "BEGIN", {"ops": len(ops)})
        for op in ops:
            self._append_wal(xid, "OP", op)
        self._append_wal(xid, "COMMIT", {"ops": len(ops)})

    def _append_wal(self, xid: int, kind: str, payload: Dict[str, Any]) -> None:
        record = {
            "lsn": self.next_lsn,
            "xid": xid,
            "kind": kind,
            "payload": payload,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        with self.wal_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.next_lsn += 1

    def _recover_from_wal(self) -> None:
        if not self.wal_path.exists():
            return

        tx_ops: Dict[int, List[Dict[str, Any]]] = {}
        committed: List[int] = []
        max_seen_xid = self.state["applied_xid"]

        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                xid = int(rec.get("xid", 0))
                max_seen_xid = max(max_seen_xid, xid)
                kind = rec.get("kind")
                if kind == "OP":
                    tx_ops.setdefault(xid, []).append(rec.get("payload", {}))
                elif kind == "COMMIT":
                    committed.append(xid)

        changed = False
        for xid in sorted(set(committed)):
            if xid <= self.state["applied_xid"]:
                continue
            ops = tx_ops.get(xid, [])
            self._apply_ops(ops, xid)
            self.state["applied_xid"] = xid
            changed = True

        self.state["next_xid"] = max(self.state["next_xid"], max_seen_xid + 1)
        if changed:
            self._save_state()
        else:
            self._save_state()

    def _visible_rows(self, table: str, snapshot_xid: int) -> List[Dict[str, Any]]:
        rows = []
        for v in self._load_versions(table):
            if self._visible_committed(v, snapshot_xid):
                rows.append({"rid": v["rid"], "data": v["data"]})
        return rows

    @staticmethod
    def _visible_committed(version: Dict[str, Any], snapshot_xid: int) -> bool:
        if version["xmin"] > snapshot_xid:
            return False
        xmax = version.get("xmax")
        if xmax is not None and xmax <= snapshot_xid:
            return False
        return True

    @staticmethod
    def _visible_for_apply(version: Dict[str, Any], base_snapshot: int, own_xid: int) -> bool:
        xmin = version["xmin"]
        if xmin != own_xid and xmin > base_snapshot:
            return False
        xmax = version.get("xmax")
        if xmax is None:
            return True
        if xmax == own_xid:
            return False
        return xmax > base_snapshot

    def _matches_where(self, row: Dict[str, Any], where: List[Dict[str, Any]]) -> bool:
        for cond in where:
            lhs = row.get(cond["column"])
            rhs = cond["value"]
            op = cond["op"]
            if not self._compare(lhs, rhs, op):
                return False
        return True

    @staticmethod
    def _compare(lhs: Any, rhs: Any, op: str) -> bool:
        if op == "=":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs

        if lhs is None or rhs is None:
            return False

        if op == ">":
            return lhs > rhs
        if op == ">=":
            return lhs >= rhs
        if op == "<":
            return lhs < rhs
        if op == "<=":
            return lhs <= rhs
        raise MiniPgError(f"Unsupported operator: {op}")

    def _project_rows(self, rows: List[Dict[str, Any]], columns: List[str], schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        if columns == ["*"]:
            ordered_cols = [c["name"] for c in schema["columns"]]
            return [{c: row.get(c) for c in ordered_cols} for row in rows]
        return [{c: row.get(c) for c in columns} for row in rows]

    @staticmethod
    def _where_text(where: List[Dict[str, Any]]) -> str:
        if not where:
            return "TRUE"
        parts = [f"{w['column']} {w['op']} {repr(w['value'])}" for w in where]
        return " AND ".join(parts)

    def _coerce(self, value: Any, target_type: str) -> Any:
        if value is None:
            return None
        if target_type == "TEXT":
            return str(value)
        if target_type == "INT":
            if isinstance(value, bool):
                raise MiniPgError("Cannot coerce BOOL to INT")
            try:
                return int(value)
            except Exception as exc:  # noqa: BLE001
                raise MiniPgError(f"Value {value!r} cannot be coerced to INT") from exc
        if target_type == "FLOAT":
            if isinstance(value, bool):
                raise MiniPgError("Cannot coerce BOOL to FLOAT")
            try:
                return float(value)
            except Exception as exc:  # noqa: BLE001
                raise MiniPgError(f"Value {value!r} cannot be coerced to FLOAT") from exc
        if target_type == "BOOL":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                low = value.strip().lower()
                if low in {"true", "t", "1"}:
                    return True
                if low in {"false", "f", "0"}:
                    return False
            raise MiniPgError(f"Value {value!r} cannot be coerced to BOOL")
        raise MiniPgError(f"Unsupported target type: {target_type}")

    def _validate_select_columns(self, schema: Dict[str, Any], columns: List[str]) -> None:
        if columns == ["*"]:
            return
        for c in columns:
            self._require_column(schema, c)

    def _validate_where(self, schema: Dict[str, Any], where: List[Dict[str, Any]]) -> None:
        type_by_col = {c["name"]: c["type"] for c in schema["columns"]}
        for cond in where:
            col = cond["column"]
            self._require_column(schema, col)
            cond["value"] = self._coerce(cond["value"], type_by_col[col])

    def _require_column(self, schema: Dict[str, Any], column: str) -> None:
        if column not in {c["name"] for c in schema["columns"]}:
            raise MiniPgError(f"Unknown column: {column}")

    def _require_table(self, table: str) -> Dict[str, Any]:
        schema = self.catalog["tables"].get(table)
        if schema is None:
            raise MiniPgError(f"Unknown table: {table}")
        return schema

    def _index_for_column(self, table: str, column: str) -> Optional[str]:
        for idx_name, idx in self.catalog["indexes"].items():
            if idx["table"] == table and idx["column"] == column:
                return idx_name
        return None

    def _indexes_by_column(self, table: str) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for idx_name, idx in self.catalog["indexes"].items():
            if idx["table"] != table:
                continue
            result.setdefault(idx["column"], []).append(idx_name)
        return result

    def _rebuild_indexes_for_table(self, table: str, snapshot_xid: Optional[int] = None) -> None:
        for idx_name, idx in self.catalog["indexes"].items():
            if idx["table"] == table:
                self._rebuild_index(idx_name, snapshot_xid=snapshot_xid)

    def _rebuild_index(self, index_name: str, snapshot_xid: Optional[int] = None) -> None:
        idx = self.catalog["indexes"][index_name]
        table = idx["table"]
        column = idx["column"]
        snapshot = self.state["applied_xid"] if snapshot_xid is None else snapshot_xid
        rows = self._visible_rows(table, snapshot)

        mapping: Dict[str, List[int]] = {}
        for row in rows:
            token = self._key_token(row["data"].get(column))
            mapping.setdefault(token, []).append(row["rid"])

        for k in mapping:
            mapping[k] = sorted(set(mapping[k]))

        payload = {"table": table, "column": column, "map": mapping}
        with self._index_path(index_name).open("w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), sort_keys=True)

    def _load_index_data(self, index_name: str) -> Dict[str, Any]:
        path = self._index_path(index_name)
        if not path.exists():
            self._rebuild_index(index_name)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _key_token(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    def _allocate_rid(self, table: str) -> int:
        schema = self.catalog["tables"][table]
        rid = schema["next_rid"]
        schema["next_rid"] += 1
        self._save_catalog()
        return rid

    def _load_versions(self, table: str) -> List[Dict[str, Any]]:
        path = self._table_path(table)
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def _save_versions(self, table: str, versions: List[Dict[str, Any]]) -> None:
        path = self._table_path(table)
        with path.open("w", encoding="utf-8") as f:
            for v in versions:
                f.write(json.dumps(v, separators=(",", ":"), sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _table_path(self, table: str) -> Path:
        return self.data_dir / f"table_{table}.jsonl"

    def _index_path(self, index_name: str) -> Path:
        return self.data_dir / f"index_{index_name}.json"

    def _load_catalog(self) -> Dict[str, Any]:
        if not self.catalog_path.exists():
            payload = {"tables": {}, "indexes": {}}
            with self.catalog_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            return payload
        with self.catalog_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save_catalog(self) -> None:
        with self.catalog_path.open("w", encoding="utf-8") as f:
            json.dump(self.catalog, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _load_state(self) -> Dict[str, int]:
        if not self.state_path.exists():
            payload = {"next_xid": 1, "applied_xid": 0}
            with self.state_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            return payload
        with self.state_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save_state(self) -> None:
        with self.state_path.open("w", encoding="utf-8") as f:
            json.dump(self.state, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _next_xid(self) -> int:
        xid = int(self.state["next_xid"])
        self.state["next_xid"] = xid + 1
        self._save_state()
        return xid

    def _load_next_lsn(self) -> int:
        if not self.wal_path.exists():
            return 1
        last = 0
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                last = max(last, int(rec.get("lsn", 0)))
        return last + 1


class MiniDynamoDB:
    """DynamoDB-inspired educational engine.

    This focuses on:
    - key-value data model with partition/sort keys
    - query-vs-scan access behavior
    - eventually consistent global secondary indexes (GSI)
    - WAL-backed durability and replay
    """

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.catalog_path = self.data_dir / "ddb_catalog.json"
        self.state_path = self.data_dir / "ddb_state.json"
        self.wal_path = self.data_dir / "ddb_wal.log"

        self.parser = DynamoCommandParser()
        self.catalog = self._load_catalog()
        self.state = self._load_state()
        self.next_lsn = self._load_next_lsn()

        self._recover_from_wal()

    def execute(self, command: str) -> ExecResult:
        stmt = self.parser.parse(command)
        stype = stmt["type"]

        if stype == "help":
            return ExecResult(self.help_text())
        if stype == "quit":
            return ExecResult("QUIT")
        if stype == "show_tables":
            return self.show_tables()
        if stype == "describe":
            return self.describe_table(stmt["table"])
        if stype == "explain":
            return self.explain(stmt["statement"])
        if stype == "tick":
            return self.tick(stmt["count"])
        if stype == "create_table":
            self._autocommit_ops([{"kind": "create_table", **stmt}])
            return ExecResult(f"Table {stmt['table']} created")
        if stype == "create_gsi":
            self._autocommit_ops([{"kind": "create_gsi", **stmt}])
            return ExecResult(f"GSI {stmt['index']} created")
        if stype == "put":
            affected = self._autocommit_ops([{"kind": "put", **stmt}])
            return ExecResult(f"PUT wrote {affected} item(s)")
        if stype == "update":
            affected = self._autocommit_ops([{"kind": "update", **stmt}])
            return ExecResult(f"UPDATE wrote {affected} item(s)")
        if stype == "delete":
            affected = self._autocommit_ops([{"kind": "delete", **stmt}])
            return ExecResult(f"DELETE removed {affected} item(s)")
        if stype == "get":
            rows, plan = self._run_get(stmt)
            msg = f"GET returned {len(rows)} item(s)"
            if not stmt["consistent"]:
                msg += " (eventual mode)"
            return ExecResult(msg, rows=rows, plan=plan)
        if stype == "query":
            rows, plan = self._run_query(stmt)
            msg = f"QUERY returned {len(rows)} item(s)"
            if not stmt["consistent"]:
                msg += " (eventual mode)"
            return ExecResult(msg, rows=rows, plan=plan)
        if stype == "scan":
            rows, plan = self._run_scan(stmt)
            return ExecResult(f"SCAN returned {len(rows)} item(s)", rows=rows, plan=plan)
        if stype == "query_gsi":
            rows, plan = self._run_query_gsi(stmt)
            msg = f"QUERY GSI returned {len(rows)} item(s)"
            if plan.get("stale_possible"):
                msg += " (index may be stale; run TICK)"
            return ExecResult(msg, rows=rows, plan=plan)

        raise MiniDynamoError(f"Unhandled Dynamo command type: {stype}")

    @staticmethod
    def model_name() -> str:
        return "DynamoDB (toy)"

    @staticmethod
    def prompt_tag() -> str:
        return "mini-ddb"

    @staticmethod
    def concept_lines() -> List[str]:
        return [
            "Concepts:",
            "  Partition key routes item placement.",
            "  Sort key enables ordered item collections.",
            "  QUERY uses keys; SCAN reads every item.",
            "  GSIs are eventually consistent (TICK to refresh).",
        ]

    @staticmethod
    def example_commands_line() -> str:
        return "Examples: CREATE TABLE PK/SK, PUT, GET, QUERY, CREATE GSI, QUERY GSI, TICK"

    def help_text(self) -> str:
        return "\n".join(
            [
                "DynamoDB-inspired commands:",
                "  CREATE TABLE orders PK user_id SK order_id",
                "  CREATE GSI gsi_email ON orders (email)",
                "  PUT orders user_id=1 order_id=101 email='a@x' total=50",
                "  GET orders user_id=1 order_id=101 CONSISTENT",
                "  QUERY orders user_id=1 LIMIT 10",
                "  QUERY orders user_id=1 SK_BEGINS '10'",
                "  SCAN orders WHERE total >= 50 LIMIT 20",
                "  UPDATE orders user_id=1 order_id=101 SET total=75 status='paid'",
                "  DELETE orders user_id=1 order_id=101",
                "  QUERY GSI gsi_email email='a@x' LIMIT 10",
                "  TICK [n]  # refresh eventual GSI backlog",
                "  SHOW TABLES / DESCRIBE orders / EXPLAIN <GET|QUERY|SCAN|QUERY GSI>",
                "  HELP / QUIT",
            ]
        )

    def runtime_status_lines(self) -> List[str]:
        backlog_counts: Dict[str, int] = {}
        for idx in self.state["gsi_backlog"]:
            backlog_counts[idx] = backlog_counts.get(idx, 0) + 1

        lines = [
            f"data dir     : {self.data_dir}",
            f"applied txid : {self.state['applied_txid']}",
            f"next txid    : {self.state['next_txid']}",
            f"tables       : {len(self.catalog['tables'])}",
            f"gsis         : {len(self.catalog['gsis'])}",
            f"gsi backlog  : {len(self.state['gsi_backlog'])}",
        ]

        for table in sorted(self.catalog["tables"].keys()):
            meta = self.catalog["tables"][table]
            items = self._load_items(table)
            gsis = [idx for idx, g in self.catalog["gsis"].items() if g["table"] == table]
            table_backlog = sum(backlog_counts.get(idx, 0) for idx in gsis)
            sk = meta["sk"] or "-"
            lines.append(
                f"{table:<12}: pk={meta['pk']}, sk={sk}, items={len(items)}, gsis={len(gsis)}, backlog={table_backlog}"
            )
        return lines

    def show_tables(self) -> ExecResult:
        rows = []
        for table in sorted(self.catalog["tables"].keys()):
            meta = self.catalog["tables"][table]
            gsis = [idx for idx, g in self.catalog["gsis"].items() if g["table"] == table]
            rows.append(
                {
                    "table": table,
                    "pk": meta["pk"],
                    "sk": meta["sk"] or "-",
                    "items": len(self._load_items(table)),
                    "gsis": ",".join(sorted(gsis)) or "-",
                }
            )
        return ExecResult(f"{len(rows)} table(s)", rows=rows)

    def describe_table(self, table: str) -> ExecResult:
        meta = self._require_table(table)
        rows: List[Dict[str, Any]] = [
            {"property": "partition_key", "value": meta["pk"]},
            {"property": "sort_key", "value": meta["sk"] or "-"},
            {"property": "read_capacity_units", "value": meta["rcu"]},
            {"property": "write_capacity_units", "value": meta["wcu"]},
            {"property": "item_count", "value": len(self._load_items(table))},
        ]
        for idx, gsi in sorted(self.catalog["gsis"].items()):
            if gsi["table"] != table:
                continue
            rows.append(
                {
                    "property": f"gsi:{idx}",
                    "value": f"attribute={gsi['attribute']}, projection={gsi['projection']}",
                }
            )
        return ExecResult(f"Schema for {table}", rows=rows)

    def explain(self, stmt: Dict[str, Any]) -> ExecResult:
        stype = stmt["type"]
        if stype == "get":
            plan = {
                "node": "Key Lookup",
                "table": stmt["table"],
                "access": "Primary key hash lookup",
                "complexity": "O(1)",
            }
            return ExecResult(f"Key Lookup on {stmt['table']} (O(1))", plan=plan)
        if stype == "query":
            plan = {
                "node": "Partition Query",
                "table": stmt["table"],
                "partition_condition": f"{stmt['key_cond']['column']} = {stmt['key_cond']['value']!r}",
                "sort_prefix": stmt["sk_begins"],
                "limit": stmt["limit"],
            }
            return ExecResult(f"Partition Query on {stmt['table']} using key condition", plan=plan)
        if stype == "scan":
            where = self._where_text(stmt["where"])
            plan = {"node": "Table Scan", "table": stmt["table"], "filter": where, "limit": stmt["limit"]}
            return ExecResult(f"Table Scan on {stmt['table']} (filter={where})", plan=plan)
        if stype == "query_gsi":
            index_meta = self._require_gsi(stmt["index"])
            stale_possible = stmt["index"] in self.state["gsi_backlog"]
            plan = {
                "node": "GSI Query",
                "index": stmt["index"],
                "table": index_meta["table"],
                "attribute": index_meta["attribute"],
                "stale_possible": stale_possible,
            }
            return ExecResult(f"GSI Query using {stmt['index']} (stale_possible={stale_possible})", plan=plan)
        raise MiniDynamoError("EXPLAIN supports GET/QUERY/SCAN/QUERY GSI only")

    def tick(self, count: int) -> ExecResult:
        processed: List[str] = []
        while self.state["gsi_backlog"] and len(processed) < count:
            index_name = self.state["gsi_backlog"].pop(0)
            if index_name in self.catalog["gsis"]:
                self._rebuild_gsi(index_name)
                processed.append(index_name)
        self._save_state()

        if processed:
            touched = ",".join(sorted(set(processed)))
            return ExecResult(
                f"TICK processed {len(processed)} refresh event(s) on [{touched}], backlog={len(self.state['gsi_backlog'])}"
            )
        return ExecResult("TICK processed 0 refresh events (backlog already empty)")

    def _run_get(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table_meta = self._require_table(stmt["table"])
        key_token, _ = self._resolve_key_token(table_meta, stmt["key"], strict=True)
        items = self._load_items(stmt["table"])
        item = items.get(key_token)
        rows = [item] if item is not None else []
        plan = {
            "node": "Key Lookup",
            "table": stmt["table"],
            "consistent": stmt["consistent"],
        }
        return rows, plan

    def _run_query(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = stmt["table"]
        table_meta = self._require_table(table)
        pk_name = table_meta["pk"]
        if stmt["key_cond"]["column"] != pk_name:
            raise MiniDynamoError(
                f"QUERY must use partition key '{pk_name}'. Received '{stmt['key_cond']['column']}'"
            )
        items = self._load_items(table)

        matched = [item for item in items.values() if item.get(pk_name) == stmt["key_cond"]["value"]]
        sk_name = table_meta["sk"]
        if sk_name:
            matched.sort(key=lambda item: (item.get(sk_name) is None, str(item.get(sk_name))))
            if stmt["sk_begins"] is not None:
                prefix = str(stmt["sk_begins"])
                matched = [item for item in matched if str(item.get(sk_name, "")).startswith(prefix)]
        elif stmt["sk_begins"] is not None:
            raise MiniDynamoError("SK_BEGINS requires a table with a sort key")

        if stmt["limit"] is not None:
            matched = matched[: stmt["limit"]]

        plan = {
            "node": "Partition Query",
            "table": table,
            "partition_key": f"{pk_name} = {stmt['key_cond']['value']!r}",
            "sort_prefix": stmt["sk_begins"],
            "limit": stmt["limit"],
            "consistent": stmt["consistent"],
        }
        return matched, plan

    def _run_scan(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = stmt["table"]
        self._require_table(table)
        items = list(self._load_items(table).values())
        matched = [item for item in items if self._matches_where(item, stmt["where"])]
        if stmt["limit"] is not None:
            matched = matched[: stmt["limit"]]
        plan = {
            "node": "Table Scan",
            "table": table,
            "filter": self._where_text(stmt["where"]),
            "limit": stmt["limit"],
            "estimated_items": len(items),
        }
        return matched, plan

    def _run_query_gsi(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        index_name = stmt["index"]
        index_meta = self._require_gsi(index_name)
        column = stmt["attr_cond"]["column"]
        if column != index_meta["attribute"]:
            raise MiniDynamoError(
                f"QUERY GSI {index_name} must filter on attribute '{index_meta['attribute']}', got '{column}'"
            )

        gsi_data = self._load_gsi_data(index_name)
        token = self._key_token(stmt["attr_cond"]["value"])
        key_tokens = gsi_data.get("map", {}).get(token, [])
        if stmt["limit"] is not None:
            key_tokens = key_tokens[: stmt["limit"]]

        table = index_meta["table"]
        items = self._load_items(table)
        rows = [items[k] for k in key_tokens if k in items]
        stale_possible = index_name in self.state["gsi_backlog"]
        plan = {
            "node": "GSI Query",
            "index": index_name,
            "table": table,
            "attribute": index_meta["attribute"],
            "limit": stmt["limit"],
            "stale_possible": stale_possible,
        }
        return rows, plan

    def _autocommit_ops(self, ops: List[Dict[str, Any]]) -> int:
        txid = self._next_txid()
        self._write_tx_wal(txid, ops)
        affected = self._apply_ops(ops, txid)
        self.state["applied_txid"] = txid
        self._save_state()
        return affected

    def _apply_ops(self, ops: List[Dict[str, Any]], txid: int) -> int:
        affected = 0
        touched_tables = set()

        for op in ops:
            kind = op["kind"]
            if kind == "create_table":
                table = op["table"]
                existing = self.catalog["tables"].get(table)
                desired = {"pk": op["pk"], "sk": op["sk"], "rcu": op["rcu"], "wcu": op["wcu"]}
                if existing is not None:
                    if existing != desired:
                        raise MiniDynamoError(f"Table {table} already exists with different definition")
                else:
                    self.catalog["tables"][table] = desired
                    self._save_catalog()
                    self._save_items(table, {})
                continue

            if kind == "create_gsi":
                index = op["index"]
                table = op["table"]
                self._require_table(table)
                desired = {
                    "table": table,
                    "attribute": op["attribute"],
                    "projection": op["projection"],
                }
                existing = self.catalog["gsis"].get(index)
                if existing is not None:
                    if existing != desired:
                        raise MiniDynamoError(f"GSI {index} already exists with different definition")
                else:
                    self.catalog["gsis"][index] = desired
                    self._save_catalog()
                self._rebuild_gsi(index)
                continue

            if kind == "put":
                table = op["table"]
                table_meta = self._require_table(table)
                key_token, _ = self._resolve_key_token(table_meta, op["item"], strict=False)
                items = self._load_items(table)
                items[key_token] = dict(op["item"])
                self._save_items(table, items)
                touched_tables.add(table)
                affected += 1
                continue

            if kind == "update":
                table = op["table"]
                table_meta = self._require_table(table)
                key_token, key_values = self._resolve_key_token(table_meta, op["key"], strict=True)
                items = self._load_items(table)
                item = dict(items.get(key_token, key_values))
                for key_attr in key_values:
                    if key_attr in op["set"] and op["set"][key_attr] != key_values[key_attr]:
                        raise MiniDynamoError("Cannot update primary key attributes in UPDATE")
                item.update(op["set"])
                items[key_token] = item
                self._save_items(table, items)
                touched_tables.add(table)
                affected += 1
                continue

            if kind == "delete":
                table = op["table"]
                table_meta = self._require_table(table)
                key_token, _ = self._resolve_key_token(table_meta, op["key"], strict=True)
                items = self._load_items(table)
                if key_token in items:
                    del items[key_token]
                    self._save_items(table, items)
                    affected += 1
                    touched_tables.add(table)
                continue

            raise MiniDynamoError(f"Unknown operation kind: {kind}")

        refresh_events: List[str] = []
        for table in touched_tables:
            for index_name, meta in self.catalog["gsis"].items():
                if meta["table"] == table:
                    refresh_events.append(index_name)
        if refresh_events:
            self.state["gsi_backlog"].extend(refresh_events)

        return affected

    def _write_tx_wal(self, txid: int, ops: List[Dict[str, Any]]) -> None:
        self._append_wal(txid, "BEGIN", {"ops": len(ops)})
        for op in ops:
            self._append_wal(txid, "OP", op)
        self._append_wal(txid, "COMMIT", {"ops": len(ops)})

    def _append_wal(self, txid: int, kind: str, payload: Dict[str, Any]) -> None:
        record = {
            "lsn": self.next_lsn,
            "txid": txid,
            "kind": kind,
            "payload": payload,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        with self.wal_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.next_lsn += 1

    def _recover_from_wal(self) -> None:
        if not self.wal_path.exists():
            return

        tx_ops: Dict[int, List[Dict[str, Any]]] = {}
        committed: List[int] = []
        max_seen_txid = self.state["applied_txid"]

        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                txid = int(rec.get("txid", 0))
                max_seen_txid = max(max_seen_txid, txid)
                kind = rec.get("kind")
                if kind == "OP":
                    tx_ops.setdefault(txid, []).append(rec.get("payload", {}))
                elif kind == "COMMIT":
                    committed.append(txid)

        for txid in sorted(set(committed)):
            if txid <= self.state["applied_txid"]:
                continue
            self._apply_ops(tx_ops.get(txid, []), txid)
            self.state["applied_txid"] = txid

        self.state["next_txid"] = max(self.state["next_txid"], max_seen_txid + 1)
        self._save_state()

    def _require_table(self, table: str) -> Dict[str, Any]:
        meta = self.catalog["tables"].get(table)
        if meta is None:
            raise MiniDynamoError(f"Unknown table: {table}")
        return meta

    def _require_gsi(self, index: str) -> Dict[str, Any]:
        meta = self.catalog["gsis"].get(index)
        if meta is None:
            raise MiniDynamoError(f"Unknown GSI: {index}")
        return meta

    def _resolve_key_token(
        self, table_meta: Dict[str, Any], attributes: Dict[str, Any], strict: bool
    ) -> Tuple[str, Dict[str, Any]]:
        pk = table_meta["pk"]
        sk = table_meta["sk"]
        if pk not in attributes:
            raise MiniDynamoError(f"Missing partition key attribute: {pk}")
        key_values = {pk: attributes[pk]}
        allowed = {pk}

        if sk is not None:
            if sk not in attributes:
                raise MiniDynamoError(f"Missing sort key attribute: {sk}")
            key_values[sk] = attributes[sk]
            allowed.add(sk)

        if strict:
            extra = set(attributes) - allowed
            if extra:
                extras = ", ".join(sorted(extra))
                raise MiniDynamoError(f"Unexpected non-key attribute(s): {extras}")

        token = self._key_token(key_values)
        return token, key_values

    def _rebuild_gsi(self, index_name: str) -> None:
        index_meta = self._require_gsi(index_name)
        table = index_meta["table"]
        table_meta = self._require_table(table)
        attribute = index_meta["attribute"]
        items = self._load_items(table)

        mapping: Dict[str, List[str]] = {}
        for key_token, item in items.items():
            if attribute not in item or item[attribute] is None:
                continue
            attr_token = self._key_token(item[attribute])
            mapping.setdefault(attr_token, []).append(key_token)
        for token in mapping:
            mapping[token] = sorted(set(mapping[token]))

        payload = {
            "index": index_name,
            "table": table,
            "attribute": attribute,
            "projection": index_meta["projection"],
            "pk": table_meta["pk"],
            "sk": table_meta["sk"],
            "map": mapping,
        }
        with self._gsi_path(index_name).open("w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _load_gsi_data(self, index_name: str) -> Dict[str, Any]:
        path = self._gsi_path(index_name)
        if not path.exists():
            self._rebuild_gsi(index_name)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _load_items(self, table: str) -> Dict[str, Dict[str, Any]]:
        path = self._table_path(table)
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return {}
        return payload

    def _save_items(self, table: str, items: Dict[str, Dict[str, Any]]) -> None:
        path = self._table_path(table)
        with path.open("w", encoding="utf-8") as f:
            json.dump(items, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _table_path(self, table: str) -> Path:
        return self.data_dir / f"ddb_table_{table}.json"

    def _gsi_path(self, index_name: str) -> Path:
        return self.data_dir / f"ddb_gsi_{index_name}.json"

    def _load_catalog(self) -> Dict[str, Any]:
        if not self.catalog_path.exists():
            payload = {"tables": {}, "gsis": {}}
            with self.catalog_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            return payload
        with self.catalog_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save_catalog(self) -> None:
        with self.catalog_path.open("w", encoding="utf-8") as f:
            json.dump(self.catalog, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            payload = {"next_txid": 1, "applied_txid": 0, "gsi_backlog": []}
            with self.state_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            return payload
        with self.state_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        payload.setdefault("gsi_backlog", [])
        payload.setdefault("next_txid", 1)
        payload.setdefault("applied_txid", 0)
        return payload

    def _save_state(self) -> None:
        with self.state_path.open("w", encoding="utf-8") as f:
            json.dump(self.state, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _next_txid(self) -> int:
        txid = int(self.state["next_txid"])
        self.state["next_txid"] = txid + 1
        self._save_state()
        return txid

    def _load_next_lsn(self) -> int:
        if not self.wal_path.exists():
            return 1
        last = 0
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                last = max(last, int(rec.get("lsn", 0)))
        return last + 1

    @staticmethod
    def _key_token(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _compare(lhs: Any, rhs: Any, op: str) -> bool:
        if op == "=":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs
        if lhs is None or rhs is None:
            return False
        if op == ">":
            return lhs > rhs
        if op == ">=":
            return lhs >= rhs
        if op == "<":
            return lhs < rhs
        if op == "<=":
            return lhs <= rhs
        raise MiniDynamoError(f"Unsupported operator: {op}")

    def _matches_where(self, row: Dict[str, Any], where: List[Dict[str, Any]]) -> bool:
        for cond in where:
            lhs = row.get(cond["column"])
            if not self._compare(lhs, cond["value"], cond["op"]):
                return False
        return True

    @staticmethod
    def _where_text(where: List[Dict[str, Any]]) -> str:
        if not where:
            return "TRUE"
        return " AND ".join(f"{c['column']} {c['op']} {c['value']!r}" for c in where)


class MiniVectorDB:
    """Vector database inspired educational engine.

    This focuses on:
    - embedding vectors + metadata storage
    - exact top-k search
    - ANN-like partition index with probe control
    - WAL-backed durability and crash recovery
    """

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.catalog_path = self.data_dir / "vdb_catalog.json"
        self.state_path = self.data_dir / "vdb_state.json"
        self.wal_path = self.data_dir / "vdb_wal.log"

        self.parser = VectorCommandParser()
        self.catalog = self._load_catalog()
        self.state = self._load_state()
        self.next_lsn = self._load_next_lsn()

        self._recover_from_wal()

    def execute(self, command: str) -> ExecResult:
        stmt = self.parser.parse(command)
        stype = stmt["type"]

        if stype == "help":
            return ExecResult(self.help_text())
        if stype == "quit":
            return ExecResult("QUIT")
        if stype == "show_collections":
            return self.show_collections()
        if stype == "describe":
            return self.describe_collection(stmt["collection"])
        if stype == "explain":
            return self.explain(stmt["statement"])
        if stype == "tick":
            return self.tick(stmt["count"])
        if stype == "rebuild_index":
            collection = stmt["collection"]
            self._require_collection(collection)
            self._rebuild_index(collection)
            self._remove_from_backlog(collection, remove_all=True)
            self._save_state()
            return ExecResult(f"REBUILD INDEX {collection} completed")
        if stype == "create_collection":
            self._autocommit_ops([{"kind": "create_collection", **stmt}])
            return ExecResult(f"Collection {stmt['collection']} created")
        if stype == "drop_collection":
            affected = self._autocommit_ops([{"kind": "drop_collection", **stmt}])
            if affected:
                return ExecResult(f"Collection {stmt['collection']} dropped")
            return ExecResult(f"Collection {stmt['collection']} did not exist")
        if stype == "upsert":
            affected = self._autocommit_ops([{"kind": "upsert", **stmt}])
            return ExecResult(f"UPSERT wrote {affected} vector(s)")
        if stype == "delete":
            affected = self._autocommit_ops([{"kind": "delete", **stmt}])
            return ExecResult(f"DELETE removed {affected} vector(s)")
        if stype == "get":
            rows, plan = self._run_get(stmt)
            return ExecResult(f"GET returned {len(rows)} vector(s)", rows=rows, plan=plan)
        if stype == "list":
            rows, plan = self._run_list(stmt)
            return ExecResult(f"LIST returned {len(rows)} vector(s)", rows=rows, plan=plan)
        if stype == "search":
            rows, plan = self._run_search(stmt)
            msg = f"SEARCH returned {len(rows)} vector(s)"
            if plan.get("stale_possible") and not stmt["exact"]:
                msg += " (index may be stale; run TICK or REBUILD INDEX)"
            return ExecResult(msg, rows=rows, plan=plan)

        raise MiniVectorError(f"Unhandled Vector command type: {stype}")

    @staticmethod
    def model_name() -> str:
        return "VectorDB (toy)"

    @staticmethod
    def prompt_tag() -> str:
        return "mini-vdb"

    @staticmethod
    def concept_lines() -> List[str]:
        return [
            "Concepts:",
            "  Embeddings map semantics to vectors.",
            "  ANN partitions trade recall for latency.",
            "  PROBE controls how many partitions to visit.",
            "  FILTER applies metadata constraints.",
        ]

    @staticmethod
    def example_commands_line() -> str:
        return "Examples: CREATE COLLECTION, UPSERT, SEARCH TOPK/PROBE, TICK, REBUILD INDEX"

    def help_text(self) -> str:
        return "\n".join(
            [
                "VectorDB-inspired commands:",
                "  CREATE COLLECTION docs DIM 4 METRIC COSINE PARTITIONS 8",
                "  UPSERT docs id=doc1 vector=[0.1,0.2,0.3,0.4] topic=ml lang=en",
                "  GET docs id=doc1",
                "  SEARCH docs vector=[0.1,0.2,0.3,0.4] TOPK 5 PROBE 3",
                "  SEARCH docs vector=[0.1,0.2,0.3,0.4] TOPK 5 FILTER topic=ml lang=en",
                "  SEARCH docs vector=[0.1,0.2,0.3,0.4] TOPK 5 EXACT",
                "  LIST docs LIMIT 20",
                "  DELETE docs id=doc1",
                "  TICK [n]  # refresh ANN index backlog",
                "  REBUILD INDEX docs",
                "  SHOW COLLECTIONS / DESCRIBE docs / EXPLAIN <SEARCH|GET|LIST>",
                "  DROP COLLECTION docs",
                "  HELP / QUIT",
            ]
        )

    def runtime_status_lines(self) -> List[str]:
        backlog_counts: Dict[str, int] = {}
        for collection in self.state["index_backlog"]:
            backlog_counts[collection] = backlog_counts.get(collection, 0) + 1

        lines = [
            f"data dir     : {self.data_dir}",
            f"applied txid : {self.state['applied_txid']}",
            f"next txid    : {self.state['next_txid']}",
            f"collections  : {len(self.catalog['collections'])}",
            f"idx backlog  : {len(self.state['index_backlog'])}",
        ]
        for collection in sorted(self.catalog["collections"].keys()):
            meta = self.catalog["collections"][collection]
            items = self._load_items(collection)
            queued = backlog_counts.get(collection, 0)
            freshness = "stale" if queued else "fresh"
            lines.append(
                f"{collection:<12}: dim={meta['dim']}, metric={meta['metric']}, items={len(items)}, parts={meta['partitions']}, {freshness}"
            )
        return lines

    def show_collections(self) -> ExecResult:
        rows = []
        backlog = set(self.state["index_backlog"])
        for collection in sorted(self.catalog["collections"].keys()):
            meta = self.catalog["collections"][collection]
            rows.append(
                {
                    "collection": collection,
                    "dim": meta["dim"],
                    "metric": meta["metric"],
                    "partitions": meta["partitions"],
                    "items": len(self._load_items(collection)),
                    "index_fresh": "no" if collection in backlog else "yes",
                }
            )
        return ExecResult(f"{len(rows)} collection(s)", rows=rows)

    def describe_collection(self, collection: str) -> ExecResult:
        meta = self._require_collection(collection)
        index = self._load_index(collection)
        rows = [
            {"property": "dimension", "value": meta["dim"]},
            {"property": "metric", "value": meta["metric"]},
            {"property": "partitions_configured", "value": meta["partitions"]},
            {"property": "items", "value": len(self._load_items(collection))},
            {"property": "index_partitions", "value": len(index.get("centroids", []))},
            {"property": "index_built_txid", "value": index.get("built_txid")},
            {"property": "index_stale", "value": "yes" if collection in set(self.state["index_backlog"]) else "no"},
        ]
        return ExecResult(f"Schema for {collection}", rows=rows)

    def explain(self, stmt: Dict[str, Any]) -> ExecResult:
        stype = stmt["type"]
        if stype == "get":
            plan = {"node": "Key Lookup", "collection": stmt["collection"], "complexity": "O(1)"}
            return ExecResult(f"Key Lookup on {stmt['collection']} (O(1))", plan=plan)
        if stype == "list":
            plan = {
                "node": "Collection Scan",
                "collection": stmt["collection"],
                "limit": stmt["limit"],
                "complexity": "O(n)",
            }
            return ExecResult(f"Collection Scan on {stmt['collection']} (limit={stmt['limit']})", plan=plan)
        if stype == "search":
            _, plan = self._run_search(stmt, explain_only=True)
            if plan["node"] == "ANN Search":
                msg = (
                    f"ANN Search on {plan['collection']} (probe={plan['probe']}, "
                    f"candidates={plan['candidates_scanned']}, total={plan['total_items']})"
                )
            else:
                msg = f"Exact Vector Scan on {plan['collection']} (total={plan['total_items']})"
            return ExecResult(msg, plan=plan)
        raise MiniVectorError("EXPLAIN supports SEARCH/GET/LIST only")

    def tick(self, count: int) -> ExecResult:
        processed: List[str] = []
        while self.state["index_backlog"] and len(processed) < count:
            collection = self.state["index_backlog"].pop(0)
            if collection in self.catalog["collections"]:
                self._rebuild_index(collection)
                processed.append(collection)
        self._save_state()

        if processed:
            touched = ",".join(sorted(set(processed)))
            return ExecResult(
                f"TICK processed {len(processed)} index refresh event(s) on [{touched}], backlog={len(self.state['index_backlog'])}"
            )
        return ExecResult("TICK processed 0 index refresh events (backlog already empty)")

    def _run_get(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        collection = stmt["collection"]
        self._require_collection(collection)
        item = self._load_items(collection).get(stmt["id"])
        rows = [self._present_item(item)] if item else []
        plan = {"node": "Key Lookup", "collection": collection}
        return rows, plan

    def _run_list(self, stmt: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        collection = stmt["collection"]
        self._require_collection(collection)
        items = self._load_items(collection)
        ordered = [items[k] for k in sorted(items.keys())][: stmt["limit"]]
        rows = [self._present_item(item) for item in ordered]
        plan = {
            "node": "Collection Scan",
            "collection": collection,
            "limit": stmt["limit"],
            "estimated_items": len(items),
        }
        return rows, plan

    def _run_search(self, stmt: Dict[str, Any], explain_only: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        collection = stmt["collection"]
        meta = self._require_collection(collection)
        metric = meta["metric"]
        self._validate_vector_dim(stmt["vector"], meta["dim"])

        items = self._load_items(collection)
        all_items = list(items.values())
        filtered = [item for item in all_items if self._matches_filters(item, stmt["filters"])]
        stale_possible = collection in set(self.state["index_backlog"])

        exact = bool(stmt["exact"])
        candidates = filtered
        index_used = False

        if not exact and filtered:
            index = self._load_index(collection)
            centroids = index.get("centroids", [])
            if centroids:
                candidate_ids = self._index_candidate_ids(
                    collection,
                    query_vector=stmt["vector"],
                    probe=stmt["probe"],
                    centroids=centroids,
                    buckets=index.get("buckets", {}),
                )
                candidate_set = set(candidate_ids)
                candidates = [item for item in filtered if item["id"] in candidate_set]
                index_used = True
            else:
                index_used = False

        if exact or not index_used:
            candidates = filtered

        scored = []
        for item in candidates:
            distance, score = self._distance_and_score(stmt["vector"], item["vector"], metric)
            scored.append((distance, score, item))
        scored.sort(key=lambda entry: (entry[0], entry[2]["id"]))
        top = scored[: stmt["topk"]]

        plan = {
            "node": "ANN Search" if index_used and not exact else "Exact Vector Scan",
            "collection": collection,
            "metric": metric,
            "topk": stmt["topk"],
            "probe": stmt["probe"] if index_used and not exact else None,
            "total_items": len(all_items),
            "after_filter": len(filtered),
            "candidates_scanned": len(candidates),
            "stale_possible": stale_possible if index_used and not exact else False,
            "filter_keys": sorted(stmt["filters"].keys()),
        }

        if explain_only:
            return [], plan

        rows = []
        for distance, score, item in top:
            rows.append(
                {
                    "id": item["id"],
                    "score": round(score, 6),
                    "distance": round(distance, 6),
                    "metadata": json.dumps(item["metadata"], sort_keys=True, separators=(",", ":")),
                }
            )
        return rows, plan

    def _autocommit_ops(self, ops: List[Dict[str, Any]]) -> int:
        txid = self._next_txid()
        self._write_tx_wal(txid, ops)
        affected = self._apply_ops(ops, txid)
        self.state["applied_txid"] = txid
        self._save_state()
        return affected

    def _apply_ops(self, ops: List[Dict[str, Any]], txid: int) -> int:
        affected = 0
        touched_collections = set()

        for op in ops:
            kind = op["kind"]
            if kind == "create_collection":
                collection = op["collection"]
                desired = {
                    "dim": op["dim"],
                    "metric": op["metric"],
                    "partitions": op["partitions"],
                }
                existing = self.catalog["collections"].get(collection)
                if existing is not None:
                    if existing != desired:
                        raise MiniVectorError(f"Collection {collection} already exists with different definition")
                else:
                    self.catalog["collections"][collection] = desired
                    self._save_catalog()
                    self._save_items(collection, {})
                    self._rebuild_index(collection)
                continue

            if kind == "drop_collection":
                collection = op["collection"]
                if collection not in self.catalog["collections"]:
                    continue
                del self.catalog["collections"][collection]
                self._save_catalog()
                if self._collection_path(collection).exists():
                    self._collection_path(collection).unlink()
                if self._index_path(collection).exists():
                    self._index_path(collection).unlink()
                self._remove_from_backlog(collection, remove_all=True)
                affected += 1
                continue

            if kind == "upsert":
                collection = op["collection"]
                meta = self._require_collection(collection)
                self._validate_vector_dim(op["vector"], meta["dim"])
                item_id = str(op["id"])
                items = self._load_items(collection)
                items[item_id] = {
                    "id": item_id,
                    "vector": [float(v) for v in op["vector"]],
                    "metadata": dict(op["metadata"]),
                    "updated_txid": txid,
                }
                self._save_items(collection, items)
                touched_collections.add(collection)
                affected += 1
                continue

            if kind == "delete":
                collection = op["collection"]
                self._require_collection(collection)
                items = self._load_items(collection)
                item_id = str(op["id"])
                if item_id in items:
                    del items[item_id]
                    self._save_items(collection, items)
                    touched_collections.add(collection)
                    affected += 1
                continue

            raise MiniVectorError(f"Unknown operation kind: {kind}")

        if touched_collections:
            existing = set(self.state["index_backlog"])
            for collection in sorted(touched_collections):
                if collection in existing:
                    continue
                self.state["index_backlog"].append(collection)
                existing.add(collection)
        return affected

    def _write_tx_wal(self, txid: int, ops: List[Dict[str, Any]]) -> None:
        self._append_wal(txid, "BEGIN", {"ops": len(ops)})
        for op in ops:
            self._append_wal(txid, "OP", op)
        self._append_wal(txid, "COMMIT", {"ops": len(ops)})

    def _append_wal(self, txid: int, kind: str, payload: Dict[str, Any]) -> None:
        record = {
            "lsn": self.next_lsn,
            "txid": txid,
            "kind": kind,
            "payload": payload,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        with self.wal_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.next_lsn += 1

    def _recover_from_wal(self) -> None:
        if not self.wal_path.exists():
            return

        tx_ops: Dict[int, List[Dict[str, Any]]] = {}
        committed: List[int] = []
        max_seen_txid = self.state["applied_txid"]

        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                txid = int(rec.get("txid", 0))
                max_seen_txid = max(max_seen_txid, txid)
                kind = rec.get("kind")
                if kind == "OP":
                    tx_ops.setdefault(txid, []).append(rec.get("payload", {}))
                elif kind == "COMMIT":
                    committed.append(txid)

        for txid in sorted(set(committed)):
            if txid <= self.state["applied_txid"]:
                continue
            self._apply_ops(tx_ops.get(txid, []), txid)
            self.state["applied_txid"] = txid

        self.state["next_txid"] = max(self.state["next_txid"], max_seen_txid + 1)
        self._save_state()

    def _rebuild_index(self, collection: str) -> None:
        meta = self._require_collection(collection)
        items = self._load_items(collection)
        vectors = [(item_id, item["vector"]) for item_id, item in items.items()]
        if not vectors:
            payload = {
                "collection": collection,
                "metric": meta["metric"],
                "dim": meta["dim"],
                "partitions": meta["partitions"],
                "centroids": [],
                "buckets": {},
                "built_txid": self.state["applied_txid"],
            }
            self._save_index(collection, payload)
            return

        dim = meta["dim"]
        partitions = min(meta["partitions"], len(vectors))
        step = max(1, len(vectors) // partitions)
        centroids = [vectors[(i * step) % len(vectors)][1][:] for i in range(partitions)]

        for _ in range(6):
            assignments: List[List[Tuple[str, List[float]]]] = [[] for _ in range(partitions)]
            for item_id, vector in vectors:
                best_idx = 0
                best_dist = self._distance(vector, centroids[0], meta["metric"])
                for idx in range(1, partitions):
                    dist = self._distance(vector, centroids[idx], meta["metric"])
                    if dist < best_dist:
                        best_dist = dist
                        best_idx = idx
                assignments[best_idx].append((item_id, vector))

            new_centroids: List[List[float]] = []
            for idx, bucket in enumerate(assignments):
                if not bucket:
                    new_centroids.append(centroids[idx])
                    continue
                mean = [0.0] * dim
                for _, vector in bucket:
                    for j, value in enumerate(vector):
                        mean[j] += value
                mean = [value / len(bucket) for value in mean]
                new_centroids.append(mean)

            centroids = new_centroids

        buckets: Dict[str, List[str]] = {str(i): [] for i in range(partitions)}
        for item_id, vector in vectors:
            best_idx = 0
            best_dist = self._distance(vector, centroids[0], meta["metric"])
            for idx in range(1, partitions):
                dist = self._distance(vector, centroids[idx], meta["metric"])
                if dist < best_dist:
                    best_dist = dist
                    best_idx = idx
            buckets[str(best_idx)].append(item_id)

        for key in buckets:
            buckets[key] = sorted(set(buckets[key]))

        payload = {
            "collection": collection,
            "metric": meta["metric"],
            "dim": dim,
            "partitions": meta["partitions"],
            "centroids": centroids,
            "buckets": buckets,
            "built_txid": self.state["applied_txid"],
        }
        self._save_index(collection, payload)

    def _index_candidate_ids(
        self,
        collection: str,
        query_vector: List[float],
        probe: int,
        centroids: List[List[float]],
        buckets: Dict[str, List[str]],
    ) -> List[str]:
        metric = self._require_collection(collection)["metric"]
        if not centroids:
            return []

        ranked = []
        for idx, centroid in enumerate(centroids):
            ranked.append((self._distance(query_vector, centroid, metric), idx))
        ranked.sort(key=lambda pair: pair[0])
        selected = ranked[: min(probe, len(ranked))]

        candidate_ids: List[str] = []
        seen = set()
        for _, idx in selected:
            for item_id in buckets.get(str(idx), []):
                if item_id in seen:
                    continue
                seen.add(item_id)
                candidate_ids.append(item_id)
        return candidate_ids

    def _matches_filters(self, item: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        if not filters:
            return True
        metadata = item.get("metadata", {})
        for key, value in filters.items():
            if key == "id":
                if str(item.get("id")) != str(value):
                    return False
            elif metadata.get(key) != value:
                return False
        return True

    def _distance_and_score(self, query: List[float], vector: List[float], metric: str) -> Tuple[float, float]:
        if metric == "cosine":
            dot = sum(a * b for a, b in zip(query, vector))
            nq = math.sqrt(sum(a * a for a in query))
            nv = math.sqrt(sum(b * b for b in vector))
            if nq == 0.0 or nv == 0.0:
                return 1.0, 0.0
            score = dot / (nq * nv)
            return 1.0 - score, score
        if metric == "l2":
            distance = math.sqrt(sum((a - b) ** 2 for a, b in zip(query, vector)))
            return distance, -distance
        if metric == "dot":
            score = sum(a * b for a, b in zip(query, vector))
            return -score, score
        raise MiniVectorError(f"Unknown metric: {metric}")

    def _distance(self, left: List[float], right: List[float], metric: str) -> float:
        distance, _ = self._distance_and_score(left, right, metric)
        return distance

    def _validate_vector_dim(self, vector: List[float], expected_dim: int) -> None:
        if len(vector) != expected_dim:
            raise MiniVectorError(f"Vector dimension mismatch: expected {expected_dim}, got {len(vector)}")
        for value in vector:
            if not math.isfinite(value):
                raise MiniVectorError("Vector elements must be finite numbers")

    @staticmethod
    def _present_item(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item["id"],
            "dim": len(item["vector"]),
            "vector": json.dumps(item["vector"], separators=(",", ":")),
            "metadata": json.dumps(item["metadata"], sort_keys=True, separators=(",", ":")),
        }

    def _remove_from_backlog(self, collection: str, remove_all: bool) -> None:
        if remove_all:
            self.state["index_backlog"] = [c for c in self.state["index_backlog"] if c != collection]
            return
        for idx, queued in enumerate(self.state["index_backlog"]):
            if queued == collection:
                del self.state["index_backlog"][idx]
                return

    def _require_collection(self, collection: str) -> Dict[str, Any]:
        meta = self.catalog["collections"].get(collection)
        if meta is None:
            raise MiniVectorError(f"Unknown collection: {collection}")
        return meta

    def _load_items(self, collection: str) -> Dict[str, Dict[str, Any]]:
        path = self._collection_path(collection)
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}

    def _save_items(self, collection: str, items: Dict[str, Dict[str, Any]]) -> None:
        path = self._collection_path(collection)
        with path.open("w", encoding="utf-8") as f:
            json.dump(items, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _load_index(self, collection: str) -> Dict[str, Any]:
        path = self._index_path(collection)
        if not path.exists():
            self._rebuild_index(collection)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save_index(self, collection: str, payload: Dict[str, Any]) -> None:
        path = self._index_path(collection)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _collection_path(self, collection: str) -> Path:
        return self.data_dir / f"vdb_collection_{collection}.json"

    def _index_path(self, collection: str) -> Path:
        return self.data_dir / f"vdb_index_{collection}.json"

    def _load_catalog(self) -> Dict[str, Any]:
        if not self.catalog_path.exists():
            payload = {"collections": {}}
            with self.catalog_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            return payload
        with self.catalog_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save_catalog(self) -> None:
        with self.catalog_path.open("w", encoding="utf-8") as f:
            json.dump(self.catalog, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            payload = {"next_txid": 1, "applied_txid": 0, "index_backlog": []}
            with self.state_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
            return payload
        with self.state_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        payload.setdefault("next_txid", 1)
        payload.setdefault("applied_txid", 0)
        payload.setdefault("index_backlog", [])
        return payload

    def _save_state(self) -> None:
        with self.state_path.open("w", encoding="utf-8") as f:
            json.dump(self.state, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

    def _next_txid(self) -> int:
        txid = int(self.state["next_txid"])
        self.state["next_txid"] = txid + 1
        self._save_state()
        return txid

    def _load_next_lsn(self) -> int:
        if not self.wal_path.exists():
            return 1
        last = 0
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                last = max(last, int(rec.get("lsn", 0)))
        return last + 1


def format_rows(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "(0 rows)"

    columns = list(rows[0].keys())
    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            widths[c] = max(widths[c], len(str(row.get(c))))

    sep = "+" + "+".join("-" * (widths[c] + 2) for c in columns) + "+"
    header = "| " + " | ".join(c.ljust(widths[c]) for c in columns) + " |"
    body = ["| " + " | ".join(str(row.get(c)).ljust(widths[c]) for c in columns) + " |" for row in rows]
    return "\n".join([sep, header, sep, *body, sep])


def format_exec_result(result: ExecResult) -> str:
    blocks = [result.message]
    if result.plan:
        blocks.append("plan: " + json.dumps(result.plan, sort_keys=True))
    if result.rows is not None:
        blocks.append(format_rows(result.rows))
    return "\n".join(blocks)


class MiniDbTui:
    def __init__(self, db: Any):
        self.db = db
        self.running = True
        self.logs: List[str] = [
            f"{self.db.model_name()} TUI started.",
            "Type HELP for commands. Press Esc or Ctrl+C to exit.",
        ]
        self.input_buffer = ""
        self.history: List[str] = []
        self.history_index: Optional[int] = None

    def run(self) -> None:
        curses.wrapper(self._loop)

    def _loop(self, stdscr: Any) -> None:
        curses.curs_set(1)
        stdscr.keypad(True)

        while self.running:
            self._render(stdscr)
            try:
                ch = stdscr.get_wch()
            except KeyboardInterrupt:
                break

            if isinstance(ch, str):
                if ch in {"\n", "\r"}:
                    self._submit()
                elif ch == "\x1b":
                    self.running = False
                elif ch in {"\x08", "\x7f"}:
                    self.input_buffer = self.input_buffer[:-1]
                elif ch == "\x03":
                    self.running = False
                elif ch.isprintable():
                    self.input_buffer += ch
            else:
                if ch in {curses.KEY_BACKSPACE, 127}:
                    self.input_buffer = self.input_buffer[:-1]
                elif ch == curses.KEY_UP:
                    self._history_up()
                elif ch == curses.KEY_DOWN:
                    self._history_down()

    def _submit(self) -> None:
        line = self.input_buffer.strip()
        self.input_buffer = ""
        self.history_index = None
        if not line:
            return

        self.history.append(line)
        self.logs.append(f"{self.db.prompt_tag()}=> {line}")

        try:
            result = self.db.execute(line)
            if result.message == "QUIT":
                self.running = False
                return
            self.logs.extend(format_exec_result(result).splitlines())
        except Exception as exc:  # noqa: BLE001
            self.logs.append(f"ERROR: {exc}")

        self.logs = self.logs[-600:]

    def _history_up(self) -> None:
        if not self.history:
            return
        if self.history_index is None:
            self.history_index = len(self.history) - 1
        else:
            self.history_index = max(0, self.history_index - 1)
        self.input_buffer = self.history[self.history_index]

    def _history_down(self) -> None:
        if self.history_index is None:
            return
        if self.history_index >= len(self.history) - 1:
            self.history_index = None
            self.input_buffer = ""
            return
        self.history_index += 1
        self.input_buffer = self.history[self.history_index]

    def _render(self, stdscr: Any) -> None:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        if h < 14 or w < 70:
            self._safe_add(stdscr, 0, 0, "Terminal too small. Resize to at least 70x14.")
            stdscr.refresh()
            return

        status_h = 2
        footer_h = 3
        body_top = status_h
        body_bottom = h - footer_h - 1
        right_w = max(28, w // 3)
        split_x = w - right_w - 1

        status = f"{self.db.model_name()} | prompt={self.db.prompt_tag()} | HELP for syntax"
        self._safe_add(stdscr, 0, 0, status[: w - 1])
        self._safe_add(stdscr, 1, 0, "Esc/Ctrl+C exit | Enter execute | Up/Down history")
        stdscr.hline(2, 0, "-", w)

        for y in range(body_top, body_bottom + 1):
            stdscr.addch(y, split_x, "|")

        log_width = split_x - 1
        log_height = body_bottom - body_top + 1
        wrapped_logs: List[str] = []
        for line in self.logs:
            pieces = textwrap.wrap(line, width=max(10, log_width - 1)) or [""]
            wrapped_logs.extend(pieces)
        visible_logs = wrapped_logs[-log_height:]
        for i, line in enumerate(visible_logs):
            self._safe_add(stdscr, body_top + i, 0, line[: log_width - 1])

        stats = self.db.runtime_status_lines() + [""] + self.db.concept_lines()
        stats_y = body_top
        for line in stats:
            if stats_y > body_bottom:
                break
            self._safe_add(stdscr, stats_y, split_x + 2, line[: right_w - 2])
            stats_y += 1

        footer_top = h - footer_h
        stdscr.hline(footer_top, 0, "-", w)
        prompt = f"{self.db.prompt_tag()}=> " + self.input_buffer
        if len(prompt) > w - 1:
            prompt = prompt[-(w - 1) :]
        self._safe_add(stdscr, footer_top + 1, 0, prompt)
        self._safe_add(stdscr, footer_top + 2, 0, self.db.example_commands_line()[: w - 1])
        stdscr.move(footer_top + 1, min(len(prompt), w - 2))
        stdscr.refresh()

    @staticmethod
    def _safe_add(stdscr: Any, y: int, x: int, text: str) -> None:
        try:
            stdscr.addstr(y, x, text)
        except curses.error:
            pass


def run_repl(db: Any) -> None:
    print(f"{db.model_name()} REPL. Type HELP for commands. Type QUIT to exit.")
    while True:
        try:
            line = input(f"{db.prompt_tag()}=> ")
        except EOFError:
            break
        if not line.strip():
            continue
        try:
            result = db.execute(line)
            if result.message == "QUIT":
                break
            print(format_exec_result(result))
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Postgres, DynamoDB, and VectorDB inspired mini databases with CLI TUI"
    )
    parser.add_argument(
        "--engine",
        choices=["postgres", "dynamo", "vector"],
        default="postgres",
        help="Execution engine (default: postgres)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory for catalog/data/WAL files (default: .mini_pg_data, .mini_ddb_data, or .mini_vdb_data)",
    )
    parser.add_argument(
        "--execute",
        action="append",
        default=[],
        help="Execute a command (can be provided multiple times)",
    )
    parser.add_argument(
        "--repl",
        action="store_true",
        help="Run plain REPL instead of curses TUI",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.engine == "postgres":
        db = MiniPostgres(args.data_dir or ".mini_pg_data")
    elif args.engine == "dynamo":
        db = MiniDynamoDB(args.data_dir or ".mini_ddb_data")
    else:
        db = MiniVectorDB(args.data_dir or ".mini_vdb_data")

    if args.execute:
        for command in args.execute:
            result = db.execute(command)
            if result.message == "QUIT":
                break
            print(format_exec_result(result))
        return

    if args.repl:
        run_repl(db)
        return

    app = MiniDbTui(db)
    app.run()


if __name__ == "__main__":
    main()
