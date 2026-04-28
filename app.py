import io
import json
import math
import re
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

# =============================================================================
DEFAULT_BASE_URL = "https://api.cloud.hypatos.ai"
AUTH_PATH = "/v2/auth/token"

# =============================================================================
# ENDPOINT CONFIGURATION
# Each entry defines typing metadata used for payload coercion and template CSV.
# =============================================================================

ENDPOINT_CONFIG: Dict[str, dict] = {
    "addresses": {
        "label": "Addresses",
        "path": "/v2/enrichment/addresses",
        "required_cols": ["externalId"],
        "flat_cols": [
            "externalClientId", "businessPartnerNumber", "parentBusinessPartnerNumber",
            "type", "globalLocationNumber", "companyName",
            "nameAlternative1", "nameAlternative2", "nameAlternative3", "nameAlternative4",
            "street", "addressAdditional", "postcode", "city", "postOfficeBox",
            "region", "countryCode", "transportationZone", "phoneNumber",
            "faxNumber", "email", "validFrom", "validTo",
        ],
        "bool_cols": ["isDefault"],
        "numeric_cols": ["addressOrder", "latitude", "longitude"],
        "json_cols": ["customFields", "customMetadata"],
        "template_row": {
            "externalId": "ADDR-001", "companyName": "Acme GmbH",
            "street": "Main St 1", "postcode": "10115",
            "city": "Berlin", "countryCode": "DE", "isDefault": "true",
            "latitude": "52.5200", "longitude": "13.4050",
            "customFields": "", "customMetadata": "",
        },
    },
    "approvers": {
        "label": "Approvers",
        "path": "/v2/enrichment/approvers",
        "required_cols": ["externalId"],
        "flat_cols": [
            "externalClientId", "code", "firstName", "lastName",
            "email", "phoneNumber",
        ],
        "bool_cols": ["isActive"],
        "numeric_cols": [],
        "json_cols": ["companyAssignment", "customFields", "customMetadata"],
        "template_row": {
            "externalId": "APR-001", "firstName": "Jane", "lastName": "Doe",
            "email": "jane@example.com", "isActive": "true",
            "companyAssignment": '["COMP-001"]',
            "customFields": "", "customMetadata": "",
        },
    },
    "companies": {
        "label": "Companies",
        "path": "/v2/enrichment/companies",
        "required_cols": ["externalId", "name"],
        "flat_cols": [
            "externalClientId", "code",
            "nameAlternative1", "nameAlternative2", "nameAlternative3", "nameAlternative4",
            "street", "addressAdditional", "postcode", "city", "state", "countryCode",
        ],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": ["vatIds", "taxIds", "customFields", "customMetadata"],
        "template_row": {
            "externalId": "COMP-001", "name": "Acme GmbH",
            "code": "AC001", "countryCode": "DE",
            "vatIds": '[{"id":"DE123456789","countryCode":"DE"}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "contracts": {
        "label": "Contracts",
        "path": "/v2/enrichment/contracts",
        "required_cols": ["externalId", "documents"],
        "flat_cols": [
            "externalClientId", "documentId", "contractNumber", "status",
            "businessPartnerContractNumber", "externalCompanyId", "title",
            "description", "createdDate", "startDate", "endDate",
            "fiscalYearLabel", "type", "subType", "currency",
        ],
        "bool_cols": [],
        "numeric_cols": [
            "totalNetAmount", "totalTaxAmount", "totalGrossAmount",
            "targetQuantity", "targetValue",
        ],
        "json_cols": [
            "documents", "salesOrganizationInfo", "businessPartner",
            "paymentTerms", "parentContract", "contractItems",
            "additionalAgreements", "obligations", "contacts",
            "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "CON-001", "contractNumber": "C-0001",
            "status": "active", "currency": "EUR",
            "documents": '[{"externalId":"DOC-1"}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "cost-centers": {
        "label": "Cost Centers",
        "path": "/v2/enrichment/cost-centers",
        "required_cols": ["externalId", "code"],
        "flat_cols": ["externalClientId"],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": [
            "companyAssignment", "type", "category", "label",
            "shortLabel", "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "CC-001", "code": "CC100",
            "companyAssignment": '["COMP-001"]',
            "label": '[{"value":"Marketing","language":"EN"}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "customers": {
        "label": "Customers",
        "path": "/v2/enrichment/customers",
        "required_cols": ["externalId", "name"],
        "flat_cols": [
            "externalClientId", "code",
            "nameAlternative1", "nameAlternative2", "nameAlternative3", "nameAlternative4",
            "street", "addressAdditional", "postcode", "city", "countryCode",
        ],
        "bool_cols": [
            "blockedForPosting", "blockedForPayment",
            "blockedForOrdering", "blockedForDelivery",
        ],
        "numeric_cols": [],
        "json_cols": [
            "vatIds", "taxIds", "customerSubsidiaries",
            "customerBankAccounts", "salesOrganizationInfo",
            "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "CUST-001", "name": "Customer AG",
            "countryCode": "US", "blockedForPosting": "false",
            "customFields": "", "customMetadata": "",
        },
    },
    "gl-accounts": {
        "label": "GL Accounts",
        "path": "/v2/enrichment/gl-accounts",
        "required_cols": ["externalId", "code"],
        "flat_cols": ["externalClientId"],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": [
            "companyAssignment", "type", "category", "label",
            "shortLabel", "accountGroup", "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "GL-001", "code": "400000",
            "companyAssignment": '["COMP-001"]',
            "label": '[{"value":"Expense Account","language":"EN"}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "goods-receipts": {
        "label": "Goods Receipts",
        "path": "/v2/enrichment/goods-receipts",
        "required_cols": ["externalId", "goodsReceiptLines"],
        "flat_cols": [
            "externalClientId", "documentId", "goodsReceiptNumber",
            "externalCompanyId", "externalSupplierId", "fiscalYearLabel",
            "issuedDate", "createdDate", "postingDate", "headerText",
            "status", "deliveryNoteNumber",
        ],
        "bool_cols": ["isReversal"],
        "numeric_cols": [],
        "json_cols": ["goodsReceiptLines", "documents", "customFields", "customMetadata"],
        "template_row": {
            "externalId": "GR-001", "goodsReceiptNumber": "GR-0001",
            "isReversal": "false",
            "goodsReceiptLines": '[{"externalId":"GRL-1","quantity":10,"unitOfMeasure":"EA"}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "internal-orders": {
        "label": "Internal Orders",
        "path": "/v2/enrichment/internal-orders",
        "required_cols": ["externalId", "internalOrderCode"],
        "flat_cols": [
            "externalClientId", "internalOrderReferenceNumber", "createdDate",
            "fiscalYearLabel", "language", "externalCompanyId", "status", "type",
            "subType", "description", "shortDescription", "profitCenterCode",
            "costCenterCodeForSettlement", "glAccountCodeForSettlement", "currency",
        ],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": ["customFields", "customMetadata"],
        "template_row": {
            "externalId": "IO-001", "internalOrderCode": "ORD-100",
            "status": "OPEN", "currency": "EUR",
            "customFields": "", "customMetadata": "",
        },
    },
    "invoices": {
        "label": "Invoices",
        "path": "/v2/enrichment/invoices",
        "required_cols": ["externalId", "invoiceLines"],
        "flat_cols": [
            "externalClientId", "documentId", "supplierInvoiceNumber",
            "invoiceNumber", "externalCompanyId", "externalSupplierId",
            "externalBankAccountId", "fiscalYearLabel", "issuedDate",
            "receivedDate", "postingDate", "externalCustomerId",
            "relatedInvoice", "currency", "externalApproverId",
            "headerText", "type", "documentType",
        ],
        "bool_cols": ["isCanceled", "isCreditNote"],
        "numeric_cols": [
            "totalNetAmount", "totalFreightCharges", "totalOtherCharges",
            "totalTaxAmount", "totalGrossAmount",
        ],
        "json_cols": [
            "invoiceLines", "paymentTerms", "withholdingTax",
            "documents", "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "INV-001", "currency": "EUR",
            "isCreditNote": "false",
            "totalGrossAmount": "119.00", "totalNetAmount": "100.00",
            "totalTaxAmount": "19.00",
            "invoiceLines": '[{"externalId":"LINE-1","netAmount":100,"quantity":1,"accountAssignments":[{"glAccountCode":"400000"}]}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "lookup-table-rows": {
        "label": "Lookup Table Rows",
        "path": "/v2/enrichment/lookup-tables",  # /{type} appended at runtime
        "required_cols": ["externalId"],
        "flat_cols": ["externalClientId", "code"],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": ["customFields", "customMetadata"],
        "template_row": {
            "externalId": "LT-001", "code": "K100",
            "dimension1": "value1", "dimension2": "value2",
            "customFields": "", "customMetadata": "",
        },
    },
    "products": {
        "label": "Products",
        "path": "/v2/enrichment/products",
        "required_cols": ["externalId"],
        "flat_cols": [
            "externalClientId", "productNumber", "type", "group", "description",
            "shortDescription", "manufacturer", "unspsc", "ean", "upc",
            "customsTariffNumber", "baseUnitOfMeasure", "weightUnit", "volumeUnit", "status",
        ],
        "bool_cols": [],
        "numeric_cols": ["grossWeight", "netWeight", "volume"],
        "json_cols": ["plants", "salesOrganizationInfo", "customFields", "customMetadata"],
        "template_row": {
            "externalId": "PROD-001", "productNumber": "P-0001",
            "description": "Widget A", "baseUnitOfMeasure": "EA",
            "grossWeight": "1.5", "weightUnit": "KG",
            "customFields": "", "customMetadata": "",
        },
    },
    "profit-centers": {
        "label": "Profit Centers",
        "path": "/v2/enrichment/profit-centers",
        "required_cols": ["externalId", "code"],
        "flat_cols": ["externalClientId", "department"],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": ["companyAssignment", "label", "shortLabel", "customFields", "customMetadata"],
        "template_row": {
            "externalId": "PC-001", "code": "PC100", "department": "Sales",
            "companyAssignment": '["COMP-001"]',
            "customFields": "", "customMetadata": "",
        },
    },
    "projects": {
        "label": "Projects",
        "path": "/v2/enrichment/projects",
        "required_cols": ["externalId", "code"],
        "flat_cols": [
            "externalClientId", "projectReferenceNumber", "createdDate",
            "fiscalYearLabel", "language", "externalCompanyId", "status",
            "startDate", "endDate", "type", "description", "shortDescription",
            "profitCenterCode", "responsibleCostCenterCode",
            "costCenterCodeForPosting", "currency", "subProjectCode", "parentProjectCode",
        ],
        "bool_cols": [],
        "numeric_cols": [],
        "json_cols": ["customFields", "customMetadata"],
        "template_row": {
            "externalId": "PRJ-001", "code": "P100",
            "status": "OPEN", "currency": "EUR",
            "startDate": "2024-01-01", "endDate": "2024-12-31",
            "customFields": "", "customMetadata": "",
        },
    },
    "purchase-orders": {
        "label": "Purchase Orders",
        "path": "/v2/enrichment/purchase-orders",
        "required_cols": ["externalId"],
        "flat_cols": [
            "externalClientId", "purchaseOrderNumber", "createdDate",
            "fiscalYearLabel", "language", "externalCompanyId", "type",
            "externalSupplierId", "status", "currency", "productNumber",
            "externalPurchaserId",
        ],
        "bool_cols": [],
        "numeric_cols": [
            "totalNetAmount", "totalFreightCharges", "totalOtherCharges",
            "totalTaxAmount", "totalGrossAmount",
        ],
        "json_cols": [
            "paymentTerms", "purchaseOrderLines", "deliveryTerms",
            "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "PO-001", "purchaseOrderNumber": "PO-0001",
            "currency": "EUR", "status": "OPEN",
            "totalGrossAmount": "1190.00",
            "purchaseOrderLines": '[{"externalId":"POL-1","quantity":5,"netAmount":1000}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "quotes": {
        "label": "Quotes",
        "path": "/v2/enrichment/quotes",
        "required_cols": ["externalId"],
        "flat_cols": [
            "externalClientId", "documentId", "quoteNumber",
            "customerOrderNumber", "externalCompanyId", "externalCustomerId",
            "status", "salesOrganizationCode", "distributionChannel",
            "division", "type", "subType", "issuedDate", "validUntilDate",
            "fiscalYearLabel", "currency", "headerText",
        ],
        "bool_cols": ["isCanceled"],
        "numeric_cols": [
            "salesGroup", "salesOffice",
            "totalNetAmount", "totalTaxAmount", "totalGrossAmount",
        ],
        "json_cols": ["paymentTerms", "quoteLines", "documents", "customFields", "customMetadata"],
        "template_row": {
            "externalId": "QUO-001", "quoteNumber": "Q-0001",
            "currency": "EUR", "isCanceled": "false",
            "validUntilDate": "2024-12-31",
            "quoteLines": '[{"externalId":"QL-1","quantity":1,"netAmount":500}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "sales-orders": {
        "label": "Sales Orders",
        "path": "/v2/enrichment/sales-orders",
        "required_cols": ["externalId"],
        "flat_cols": [
            "externalClientId", "documentId", "customerOrderNumber",
            "salesOrderNumber", "externalCompanyId", "externalCustomerId",
            "salesOrganizationCode", "distributionChannel", "division",
            "salesGroup", "salesOffice", "type", "subType", "issuedDate",
            "receivedDate", "fiscalYearLabel", "requestedDeliveryDate",
            "currency", "headerText",
        ],
        "bool_cols": ["isCanceled"],
        "numeric_cols": ["totalNetAmount", "totalTaxAmount", "totalGrossAmount"],
        "json_cols": [
            "paymentTerms", "salesOrderLines", "documents",
            "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "SO-001", "salesOrderNumber": "SO-0001",
            "currency": "EUR", "isCanceled": "false",
            "totalGrossAmount": "238.00",
            "salesOrderLines": '[{"externalId":"SOL-1","quantity":2,"netAmount":200}]',
            "customFields": "", "customMetadata": "",
        },
    },
    "suppliers": {
        "label": "Suppliers",
        "path": "/v2/enrichment/suppliers",
        "required_cols": ["externalId", "name"],
        "flat_cols": [
            "externalClientId", "code",
            "nameAlternative1", "nameAlternative2", "nameAlternative3", "nameAlternative4",
            "alternativePayee", "street", "addressAdditional", "postcode",
            "city", "state", "countryCode",
        ],
        "bool_cols": ["blockedForPosting", "blockedForPayment"],
        "numeric_cols": [],
        "json_cols": [
            "defaultAccountAssignment", "vatIds", "taxIds",
            "supplierSubsidiaries", "supplierBankAccounts",
            "customFields", "customMetadata",
        ],
        "template_row": {
            "externalId": "SUP-001", "name": "Supplier GmbH",
            "countryCode": "DE", "blockedForPosting": "false",
            "vatIds": '[{"id":"DE123456789","countryCode":"DE"}]',
            "customFields": "", "customMetadata": "",
        },
    },
}

# =============================================================================
# AUTH HELPERS (verbatim from original)
# =============================================================================

def _pick_token_field(data: dict):
    for k in ("token", "access_token", "id_token", "jwt", "bearer"):
        if k in data and data[k]:
            return data[k]
    return None


def get_token(base_url: str, client_id: str, client_secret: str, extra_headers: dict | None = None):
    """Try several auth payload/encoding variants. Returns (ok: bool, token_or_error: str)."""
    url = base_url.rstrip("/") + AUTH_PATH
    hdr_base = {"Accept": "application/json"}
    if extra_headers:
        hdr_base.update(extra_headers)

    attempts = []
    attempts.append(dict(
        desc="JSON camelCase",
        kwargs=dict(
            url=url,
            headers={**hdr_base, "Content-Type": "application/json"},
            json={"clientId": client_id, "clientSecret": client_secret},
            timeout=30,
        ),
    ))
    attempts.append(dict(
        desc="JSON snake_case",
        kwargs=dict(
            url=url,
            headers={**hdr_base, "Content-Type": "application/json"},
            json={"client_id": client_id, "client_secret": client_secret},
            timeout=30,
        ),
    ))
    attempts.append(dict(
        desc="FORM client_credentials",
        kwargs=dict(
            url=url,
            headers={**hdr_base, "Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        ),
    ))
    attempts.append(dict(
        desc="FORM camelCase",
        kwargs=dict(
            url=url,
            headers={**hdr_base, "Content-Type": "application/x-www-form-urlencoded"},
            data={"clientId": client_id, "clientSecret": client_secret},
            timeout=30,
        ),
    ))

    errors = []
    for att in attempts:
        try:
            resp = requests.post(**att["kwargs"])
            if resp.status_code < 300:
                data = resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else {}
                token = _pick_token_field(data) or data.get("token")
                if token:
                    return True, token
                if isinstance(resp.text, str) and len(resp.text) > 10 and "." in resp.text:
                    return True, resp.text.strip()
                errors.append(f"{att['desc']} OK but token missing: {data or resp.text[:300]}")
            else:
                errors.append(f"{att['desc']} -> {resp.status_code}: {resp.text[:300]}")
        except Exception as e:
            errors.append(f"{att['desc']} exception: {e}")

    return False, "Auth failed. Tried variants:\n- " + "\n- ".join(errors)


def get_auth_headers(token: str, extra: Dict[str, str] = None) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


# =============================================================================
# DATA LOADING HELPERS (verbatim from original)
# =============================================================================

def load_table(uploaded_file) -> pd.DataFrame:
    """Reads CSV/XLSX strictly as strings, strips whitespace, removes Excel apostrophe markers."""
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(
            uploaded_file,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
        )
    else:
        df = pd.read_excel(
            uploaded_file,
            dtype=str,
            keep_default_na=False,
            engine="openpyxl" if name.endswith("xlsx") else None,
        )

    df.columns = [str(c).strip() for c in df.columns]

    def _normalize(val: str) -> str:
        if val is None:
            return ""
        if isinstance(val, str):
            v = val.strip()
            if v.startswith("'") and v[1:].isdigit():
                return v[1:]
            return v
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                return ""
            return str(val)
        return str(val)

    df = df.applymap(_normalize)
    return df


def row_to_string_payload(row: pd.Series) -> dict:
    """Build a JSON-ready dict with only non-empty string values."""
    payload = {}
    for k, v in row.items():
        if v is None:
            continue
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            continue
        payload[k] = s
    return payload


# =============================================================================
# NEW HELPERS
# =============================================================================

def _slugify_type(name: str) -> str:
    if not name:
        return ""
    s = name.strip().lower().replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s


def parse_row_payload(row: pd.Series) -> Tuple[Optional[dict], str]:
    """
    Parse the JSON string from the 'value_str' column.
    Returns (payload_dict, error_message). error_message is empty on success.
    """
    raw = str(row.get("value_str", "")).strip()
    if not raw:
        return None, "Empty value_str cell"
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return None, f"value_str must be a JSON object, got {type(payload).__name__}"
        return payload, ""
    except json.JSONDecodeError as exc:
        return None, f"Invalid JSON: {exc}"


def make_template_csv(config: dict) -> str:
    """Return a CSV with a single 'value_str' column containing an example JSON payload."""
    example_payload = {
        k: v for k, v in config["template_row"].items() if v != ""
    }
    df = pd.DataFrame([{"value_str": json.dumps(example_payload, ensure_ascii=False)}])
    return df.to_csv(index=False)


def show_field_reference(config: dict) -> None:
    with st.expander("Field reference", expanded=False):
        col_left, col_right = st.columns(2)
        with col_left:
            st.markdown("**Required**")
            for f in config["required_cols"]:
                st.markdown(f"- `{f}`")
        with col_right:
            st.markdown("**Optional**")
            if config["flat_cols"]:
                st.caption("String fields")
                for f in config["flat_cols"]:
                    st.markdown(f"- `{f}`")
            if config["bool_cols"]:
                st.caption("Boolean fields (`true` / `false`)")
                for f in config["bool_cols"]:
                    st.markdown(f"- `{f}`")
            if config["numeric_cols"]:
                st.caption("Numeric fields")
                for f in config["numeric_cols"]:
                    st.markdown(f"- `{f}`")
            if config["json_cols"]:
                st.caption("JSON fields (provide as JSON string)")
                for f in config["json_cols"]:
                    st.markdown(f"- `{f}`")


def resolve_endpoint_url(endpoint_key: str, config: dict, base_url: str, lookup_type: str = "") -> str:
    if endpoint_key == "lookup-table-rows":
        return base_url.rstrip("/") + config["path"] + "/" + lookup_type
    return base_url.rstrip("/") + config["path"]


# =============================================================================
# GENERIC ENDPOINT PAGE
# =============================================================================

def page_endpoint(endpoint_key: str, config: dict, base_url: str, auth_headers: dict) -> None:
    st.subheader(config["label"])

    # Lookup table type selector (only for lookup-table-rows)
    lookup_type = ""
    if endpoint_key == "lookup-table-rows":
        type_choice = st.selectbox(
            "Lookup table type",
            options=["payment_terms", "tax_codes", "central_bank_indicator", "custom"],
            index=0,
        )
        if type_choice == "custom":
            raw_custom = st.text_input(
                "Custom lookup table type",
                placeholder="my_table",
                help="Letters, numbers, underscore only. Spaces become underscores.",
            )
            lookup_type = _slugify_type(raw_custom)
            if raw_custom and not lookup_type:
                st.error("Invalid custom type. Use only letters, numbers, or underscores.")
        else:
            lookup_type = type_choice

        if not lookup_type:
            st.info("Enter a valid lookup table type to continue.")
            return

    endpoint_url = resolve_endpoint_url(endpoint_key, config, base_url, lookup_type)
    st.caption(f"Endpoint: `{endpoint_url}`")

    # Field reference
    show_field_reference(config)

    # Template download
    template_csv = make_template_csv(config).encode("utf-8")
    st.download_button(
        label="Download template CSV",
        data=template_csv,
        file_name=f"template_{endpoint_key}.csv",
        mime="text/csv",
    )

    # File uploader
    uploaded = st.file_uploader(
        "Upload CSV or Excel file (one row = one API request)",
        type=["csv", "xlsx", "xls"],
    )
    if not uploaded:
        return

    # Load and preview
    try:
        df = load_table(uploaded)
    except Exception as exc:
        st.error(f"Failed to read file: {exc}")
        return

    st.write(f"Preview ({len(df)} rows):")
    st.dataframe(df.head(10), use_container_width=True)

    # Required column check
    if "value_str" not in df.columns:
        st.error("The file must contain a column named **value_str** with the full JSON payload per row.")
        return

    # Throttle control
    throttle_ms = st.slider("Throttle between requests (ms)", min_value=0, max_value=2000, value=0, step=50)

    if st.button(f"Send {len(df)} request(s) to {config['path']}"):
        results = []
        ok_count = 0
        error_count = 0
        progress = st.progress(0, text="Sending requests...")

        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            payload, parse_error = parse_row_payload(row)

            if parse_error:
                error_count += 1
                results.append({
                    "row": idx,
                    "status_code": "—",
                    "result": "PAYLOAD_ERROR",
                    "id": "",
                    "error": parse_error,
                })
            else:
                try:
                    resp = requests.post(endpoint_url, headers=auth_headers, json=payload, timeout=60)
                    resp_id = ""
                    try:
                        resp_json = resp.json()
                        resp_id = resp_json.get("id") or resp_json.get("externalId") or ""
                    except Exception:
                        pass

                    if resp.status_code < 300:
                        ok_count += 1
                        results.append({
                            "row": idx,
                            "status_code": resp.status_code,
                            "result": "OK",
                            "id": resp_id,
                            "error": "",
                        })
                    else:
                        error_count += 1
                        results.append({
                            "row": idx,
                            "status_code": resp.status_code,
                            "result": "ERROR",
                            "id": resp_id,
                            "error": resp.text[:2000],
                        })
                except Exception as exc:
                    error_count += 1
                    results.append({
                        "row": idx,
                        "status_code": "—",
                        "result": "ERROR",
                        "id": "",
                        "error": str(exc),
                    })

            if throttle_ms:
                time.sleep(throttle_ms / 1000.0)

            progress.progress(int(idx * 100 / len(df)), text=f"Sent {idx}/{len(df)}")

        if error_count == 0:
            st.success(f"All {ok_count} request(s) succeeded.")
        else:
            st.warning(f"Done. OK: {ok_count}, Errors: {error_count}")

        results_df = pd.DataFrame(results)
        st.dataframe(results_df, use_container_width=True)

        results_csv = results_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download results CSV",
            data=results_csv,
            file_name=f"results_{endpoint_key}.csv",
            mime="text/csv",
        )


# =============================================================================
# MAIN
# =============================================================================

def main():
    st.set_page_config(page_title="Hypatos Enrichment Uploader", layout="wide")
    st.title("Hypatos Enrichment Uploader")

    # Build sorted label → slug mapping for the selectbox
    label_to_slug = {cfg["label"]: slug for slug, cfg in ENDPOINT_CONFIG.items()}
    sorted_labels = sorted(label_to_slug.keys())

    with st.sidebar:
        st.header("Connection")

        base_url = st.text_input("Base URL", value=DEFAULT_BASE_URL)
        client_id = st.text_input("Client ID")
        client_secret = st.text_input("Client Secret", type="password")

        extra_headers_raw = st.text_area(
            "Extra headers (JSON, optional)",
            placeholder='{"X-Project-Id":"your-project-id"}',
            height=80,
        )
        extra_headers = None
        if extra_headers_raw.strip():
            try:
                extra_headers = json.loads(extra_headers_raw)
            except Exception as exc:
                st.error(f"Invalid headers JSON: {exc}")

        st.session_state["base_url"] = base_url
        st.session_state["extra_headers"] = extra_headers

        if "auth_token" not in st.session_state:
            st.session_state.auth_token = None

        if st.button("Re-authenticate") or st.session_state.auth_token is None:
            if client_id and client_secret:
                ok, token_or_err = get_token(
                    base_url=base_url,
                    client_id=client_id,
                    client_secret=client_secret,
                    extra_headers=extra_headers,
                )
                if ok:
                    st.session_state.auth_token = token_or_err
                    st.success("Authenticated.")
                else:
                    st.session_state.auth_token = None
                    st.error(token_or_err)
            else:
                st.info("Enter Client ID and Client Secret, then click Re-authenticate.")

        st.divider()

        selected_label = st.selectbox("Endpoint", options=sorted_labels)
        endpoint_key = label_to_slug[selected_label]

    token = st.session_state.get("auth_token")
    if not token:
        st.info("Enter your credentials in the sidebar and click **Re-authenticate** to continue.")
        return

    auth_headers = get_auth_headers(token, extra=st.session_state.get("extra_headers"))
    page_endpoint(endpoint_key, ENDPOINT_CONFIG[endpoint_key], base_url, auth_headers)


if __name__ == "__main__":
    main()
