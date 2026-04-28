# Hypatos Enrichment Uploader

A Streamlit app that sends bulk data to any of the 18 **Hypatos Enrichment API** endpoints by uploading a file where each row contains a full JSON payload.

---

## How it works

1. Enter your **Base URL**, **Client ID**, and **Client Secret** in the sidebar and click **Re-authenticate**
2. Select the **endpoint** you want to post to (e.g. Suppliers, Invoices, GL Accounts…)
3. Download the **template CSV** to see the expected JSON structure
4. Upload your file — one row per request, with a single column called `value_str` containing the full JSON payload
5. Click **Send** — the app posts each row and shows a results table with status codes

---

## File format

Your upload file must have **one column** named `value_str`. Each cell contains the complete JSON object for that request:

| value_str |
|-----------|
| `{"externalId":"SUP-001","name":"Acme GmbH","countryCode":"DE"}` |
| `{"externalId":"SUP-002","name":"Beta AG","countryCode":"AT"}` |

Use the **Download template CSV** button in the app to get a pre-filled example for each endpoint.

---

## Supported endpoints

| Endpoint | Required fields |
|----------|----------------|
| Addresses | `externalId` |
| Approvers | `externalId` |
| Companies | `externalId`, `name` |
| Contracts | `externalId`, `documents` |
| Cost Centers | `externalId`, `code` |
| Customers | `externalId`, `name` |
| GL Accounts | `externalId`, `code` |
| Goods Receipts | `externalId`, `goodsReceiptLines` |
| Internal Orders | `externalId`, `internalOrderCode` |
| Invoices | `externalId`, `invoiceLines` |
| Lookup Table Rows | `externalId` |
| Products | `externalId` |
| Profit Centers | `externalId`, `code` |
| Projects | `externalId`, `code` |
| Purchase Orders | `externalId` |
| Quotes | `externalId` |
| Sales Orders | `externalId` |
| Suppliers | `externalId`, `name` |

---

## Running locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

---

## Deploying to Streamlit Cloud

1. Push this repository to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in
3. Click **New app** → select your repository → set main file to `app.py`
4. Click **Deploy**

No secrets configuration is needed — credentials are entered at runtime in the sidebar.

---

## API reference

Full schema documentation: [hypatos.redocly.app/openapi/enrichment](https://hypatos.redocly.app/openapi/enrichment)
