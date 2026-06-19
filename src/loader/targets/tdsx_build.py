"""
Build a .tdsx package from a .tds file and a .hyper extract.

The TDS connection element is updated to point to the hyper file inside the package
(Data/Extracts/<hyper_basename>.hyper), then the modified TDS and the hyper file
are zipped into a .tdsx with the standard layout.

``build_tdsx_with_auto_captions`` additionally synthesizes a TDS from the Hyper
file's own schema, deriving human-readable column captions, so no hand-authored
.tds is required.
"""

import os
import re
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple


def _iter_connections(element: ET.Element) -> List[ET.Element]:
    """Collect all connection elements in tree (any namespace)."""
    out: List[ET.Element] = []
    tag = element.tag.split("}")[-1] if "}" in element.tag else element.tag
    if tag == "connection":
        out.append(element)
    for child in element:
        out.extend(_iter_connections(child))
    return out


def _find_hyper_connection(element: ET.Element) -> Optional[ET.Element]:
    """
    Find the connection element that should point to the Hyper file.
    Prefer an existing class='hyper' connection; otherwise fall back to first.
    """
    connections = _iter_connections(element)
    if not connections:
        return None
    for conn in connections:
        if (conn.get("class") or "").strip().lower() == "hyper":
            return conn
    return connections[0]


def build_tdsx(tds_path: str, hyper_path: str, output_path: Optional[str] = None) -> str:
    """
    Build a .tdsx file from a TDS and a Hyper extract.

    - Reads the TDS (XML), finds the hyper connection element, and sets
      class='hyper' and dbname='Data/Extracts/<hyper_basename>.hyper'.
    - Zips the modified TDS (at root, same name as original) and the hyper file
      at Data/Extracts/<hyper_basename>.hyper into a .tdsx.
    - Output path defaults to a temp file named <tds_stem>.tdsx if not provided.

    :param tds_path: Path to the source .tds file.
    :param hyper_path: Path to the .hyper file to embed.
    :param output_path: Optional path for the .tdsx file. If None, a temp file is used.
    :return: Path to the created .tdsx file.
    """
    if not os.path.isfile(tds_path):
        raise FileNotFoundError(f"TDS file not found: {tds_path}")
    if not os.path.isfile(hyper_path):
        raise FileNotFoundError(f"Hyper file not found: {hyper_path}")

    hyper_basename = os.path.basename(hyper_path)
    dbname_value = f"Data/Extracts/{hyper_basename}"
    tds_basename = os.path.basename(tds_path)
    tds_stem = os.path.splitext(tds_basename)[0]
    if not tds_basename.lower().endswith(".tds"):
        tds_basename = tds_stem + ".tds"

    tree = ET.parse(tds_path)
    root = tree.getroot()
    conn = _find_hyper_connection(root)
    if conn is None:
        raise ValueError("No connection element found in TDS")
    conn.set("class", "hyper")
    conn.set("dbname", dbname_value)

    if output_path is None:
        fd, output_path = tempfile.mkstemp(suffix=".tdsx", prefix="tdsx_")
        os.close(fd)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Write modified TDS from memory (preserve root attributes/ns by writing parsed tree)
        import io
        buf = io.BytesIO()
        # Do not force a default namespace; auto-generated TDS files use unqualified
        # tags and ElementTree raises ValueError when default_namespace is set.
        tree.write(buf, encoding="utf-8", xml_declaration=True, method="xml")
        zf.writestr(tds_basename, buf.getvalue())
        # Add hyper at Data/Extracts/<hyper_basename>
        zf.write(hyper_path, f"Data/Extracts/{hyper_basename}")

    return output_path


def _humanize_field_name(name: str) -> str:
    """
    Convert machine field names into readable captions.
    Examples: acceptLanguage -> Accept Language, site_id -> Site Id.
    """
    if not name:
        return ""
    s = name.replace("_", " ").replace("-", " ")
    # Split camelCase/PascalCase boundaries and acronym transitions.
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    return " ".join(part[:1].upper() + part[1:] for part in s.split() if part)


def _discover_hyper_columns(
    hyper_path: str,
    schema_name: str = "Extract",
    table_name: str = "Extract",
) -> List[str]:
    """
    Read column names from a Hyper table via pantab query.
    """
    try:
        import pantab as pt
    except Exception as e:
        raise ImportError(
            "pantab is required to auto-generate a captioned TDSX. Install with: pip install pantab"
        ) from e

    q = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
        "ORDER BY ordinal_position"
    )
    tbl = pt.frame_from_hyper_query(hyper_path, q, return_type="pyarrow")
    if tbl is None or tbl.num_rows == 0 or "column_name" not in tbl.schema.names:
        return []
    return [str(v) for v in tbl.column("column_name").to_pylist() if v is not None]


def _discover_hyper_table_schema(
    hyper_path: str,
    schema_name: str = "Extract",
    table_name: str = "Extract",
) -> List[Tuple[str, str]]:
    """
    Return [(column_name, hyper_type_name), ...] from the Hyper table.

    Requires ``tableauhyperapi`` for full type fidelity. Callers should treat an
    ImportError as "types unavailable" and fall back to name-only discovery.
    """
    try:
        from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName
    except Exception as e:
        raise ImportError(
            "tableauhyperapi is required to read Hyper column types for captioned TDSX. "
            "Install the optional extra: pip install '.[tdsx]'"
        ) from e
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path) as conn:
            table_def = conn.catalog.get_table_definition(TableName(schema_name, table_name))
    out: List[Tuple[str, str]] = []
    for col_def in table_def.columns:
        col = col_def.name.unescaped
        typ = col_def.type.tag.name.lower()
        out.append((col, typ))
    return out


def _tableau_type_triplet(hyper_type_name: Optional[str]) -> Tuple[str, str, str, str]:
    """
    Map Hyper type name to Tableau (datatype, role, type, local-type).
    """
    t = (hyper_type_name or "").lower()
    if "bool" in t:
        return ("boolean", "dimension", "nominal", "boolean")
    if "int" in t or "big" in t or "small" in t:
        return ("integer", "measure", "quantitative", "integer")
    if "numeric" in t or "double" in t or "real" in t or "decimal" in t:
        return ("real", "measure", "quantitative", "real")
    if "date" in t and "timestamp" not in t:
        return ("date", "dimension", "ordinal", "date")
    if "timestamp" in t or "time" in t:
        return ("datetime", "dimension", "ordinal", "datetime")
    return ("string", "dimension", "nominal", "string")


def build_tdsx_with_auto_captions(
    hyper_path: str,
    datasource_name: str,
    output_path: Optional[str] = None,
    schema_name: str = "Extract",
    table_name: str = "Extract",
) -> str:
    """
    Create a temporary TDS from Hyper schema with readable captions, then package as TDSX.

    Column types are read via ``tableauhyperapi`` when available; if that package is
    not installed, falls back to name-only discovery via pantab (all fields treated as
    text), so this still works with just the base dependencies.
    """
    if not os.path.isfile(hyper_path):
        raise FileNotFoundError(f"Hyper file not found: {hyper_path}")

    try:
        field_schema = _discover_hyper_table_schema(
            hyper_path,
            schema_name=schema_name,
            table_name=table_name,
        )
    except ImportError:
        field_schema = []
    if not field_schema:
        field_names = _discover_hyper_columns(
            hyper_path,
            schema_name=schema_name,
            table_name=table_name,
        )
        field_schema = [(name, "text") for name in field_names]

    root = ET.Element(
        "datasource",
        {
            "version": "18.1",
            "name": datasource_name or "datasource",
            "inline": "true",
            "formatted-name": datasource_name or "datasource",
        },
    )
    conn = ET.SubElement(
        root,
        "connection",
        {
            "class": "federated",
        },
    )
    named_conns = ET.SubElement(conn, "named-connections")
    named_conn_name = "hyper.generated"
    named_conn = ET.SubElement(
        named_conns,
        "named-connection",
        {
            "caption": datasource_name or "datasource",
            "name": named_conn_name,
        },
    )
    ET.SubElement(
        named_conn,
        "connection",
        {
            "authentication": "auth-none",
            "class": "hyper",
            "dbname": "",
            "default-settings": "yes",
            "server": "",
            "sslmode": "",
            "username": "tableau_internal_user",
            "workgroup-auth-mode": "as-is",
        },
    )
    ET.SubElement(
        conn,
        "relation",
        {
            "connection": named_conn_name,
            "name": table_name,
            "table": f"[{schema_name}].[{table_name}]",
            "type": "table",
        },
    )
    metadata_records = ET.SubElement(conn, "metadata-records")
    for ordinal, (field_name, arrow_type) in enumerate(field_schema):
        _datatype, _role, _type, local_type = _tableau_type_triplet(arrow_type)
        record = ET.SubElement(metadata_records, "metadata-record", {"class": "column"})
        ET.SubElement(record, "remote-name").text = field_name
        ET.SubElement(record, "local-name").text = f"[{field_name}]"
        ET.SubElement(record, "parent-name").text = f"[{table_name}]"
        ET.SubElement(record, "remote-alias").text = field_name
        ET.SubElement(record, "ordinal").text = str(ordinal)
        ET.SubElement(record, "local-type").text = local_type
    for field_name, arrow_type in field_schema:
        datatype, role, col_type, _local_type = _tableau_type_triplet(arrow_type)
        ET.SubElement(
            root,
            "column",
            {
                "name": f"[{field_name}]",
                "caption": _humanize_field_name(field_name),
                "datatype": datatype,
                "role": role,
                "type": col_type,
            },
        )

    with tempfile.NamedTemporaryFile(
        mode="wb",
        suffix=".tds",
        prefix="auto_caption_",
        delete=False,
    ) as tmp_tds:
        tmp_tds_path = tmp_tds.name
    try:
        tree = ET.ElementTree(root)
        tree.write(tmp_tds_path, encoding="utf-8", xml_declaration=True)
        return build_tdsx(tmp_tds_path, hyper_path, output_path=output_path)
    finally:
        try:
            os.remove(tmp_tds_path)
        except OSError:
            pass
