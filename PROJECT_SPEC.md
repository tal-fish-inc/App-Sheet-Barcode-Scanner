# Project Specification: Cannabis Packaging Inventory Web App

## 1. Project Overview
A custom Google Apps Script (GAS) Web App optimized for tablet browsers and Bluetooth/USB Barcode Scanners. The app will manage the intake, physical counts, and financial valuation of cannabis packaging and SKUs.

## 2. Core Business Logic & Rules

### A. Barcode Scanning & Double-Count Prevention
* **Hardware Assumption:** Physical barcode scanners act as keyboard emulators. The UI must feature an auto-focusing hidden/visible text input that captures the scan and triggers an `onInput` or `Enter` event. We also need an HTML5 camera fallback for tablet cameras.
* **Double-Count Logic:** Every scan must create a unique transaction ID (Barcode + Timestamp or unique serial). The backend (`Code.gs`) must query the `Scan_Logs` sheet. If a specific master case serial/barcode has already been marked "Intaken", reject the scan and alert the user visually with a red UI error.

### B. Parent-Child Relationship (Master vs. Partial Cases)
* **Master Case (Parent):** Scanning a sealed case barcode automatically logs the full unit count (e.g., 1000 units of RY-NJ-05-05).
* **Open Case (Child):** If a master case is opened, the UI must provide a "Break Case" button. This converts the Parent into a Child status, deducting the Master Case from inventory and prompting the user to manually input the physical count of remaining individual units.

### C. Intake vs. Return to Stock
* **Intake (Against PO):** Scanned items are checked against a `Purchase_Orders` table. If the scanned SKU matches the PO expectation, status updates to "Received".
* **Return to Stock:** Requires a UI toggle. Scanned items are added back to the `Total_Available` count in the inventory database.

## 3. Financial & Data Dashboard (Tablet UI)
The home screen of the Web App must be a dashboard displaying:
* **Total Inventory Value on Hand:** Calculated by multiplying `total_available` by `sku_price_per_unit` (referencing our MASTER SKU Metadata).
* **Top Selling SKUs:** Derived from `avg_weekly_quantity_ordered_past_4_weeks`.
* **Low Stock Alerts:** Highlight SKUs where `total_available` falls below a calculated threshold.

## 4. Future-Proofing & Integrations (Architecture Guidelines)
* **Messaging APIs:** Build placeholder functions in `Code.gs` for `sendSlackNotification(payload)` and `sendWhatsAppAlert(payload)` using `UrlFetchApp`. These will be triggered via Chron triggers (Time-driven) when stock dips below reorder rates.
* **External Databases (Snowflake):** Do not hardcode sheet lookups tightly. Use a centralized `getData()` and `setData()` wrapper in `Code.gs`. In the future, these wrappers will be swapped to use Google's JDBC service or REST APIs to pull sell-through rates directly from Snowflake.
* **Relabeling System:** Include a module for generating new internal Barcodes. When old packaging is scanned, it should prompt the user to "Assign New Internal Serial", linking the old vendor barcode to the new internal database ID.

## 5. Phased Rollout Plan (For Agentic Execution)

* **Phase 1: Database Setup & Backend Architecture**
  * Agent task: Define the schemas for `Inventory_Master`, `Transaction_Logs`, and `Purchase_Orders`. Draft the CRUD operations in `Code.gs`.
* **Phase 2: Frontend UI & Scanner Integration**
  * Agent task: Build the tablet-optimized frontend using an embedded CSS framework (like Bootstrap or Tailwind via CDN). Implement the auto-focus scanner input and the HTML5 camera scanner.
* **Phase 3: Core Logic Execution**
  * Agent task: Write the double-count prevention logic and the Parent/Child case breaking mechanics.
* **Phase 4: Dashboard & Financials**
  * Agent task: Build the dashboard view aggregating the cost/value data.
* **Phase 5: Webhooks & External Comm**
  * Agent task: Setup the Slack/WhatsApp webhook integrations for low-stock alerts.
