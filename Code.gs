/**
 * Cannabis Packaging Inventory Web App - Backend
 * Phase 1: Database Setup & Backend Architecture
 *
 * Architecture: All sheet I/O goes through the Data Access Layer (Section A).
 * When migrating to Snowflake/JDBC, only rewrite Section A functions.
 */

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
  SPREADSHEET_ID: '12Dt5falE1tr1DIgqbze4S6ZLbYm4Wz97eFKI7lFrwGI',
  SHEETS: {
    SKU_METADATA: 'SKU_Metadata',
    INVENTORY_MASTER: 'Inventory_Master',
    SCAN_LOGS: 'Scan_Logs',
    PURCHASE_ORDERS: 'Purchase_Orders',
    PO_LINE_ITEMS: 'PO_Line_Items',
    AUDIT_TRAIL: 'Audit_Trail'
  },
  // Primary key column name for each sheet
  PRIMARY_KEYS: {
    SKU_Metadata: 'sku_id',
    Inventory_Master: 'inventory_id',
    Scan_Logs: 'transaction_id',
    Purchase_Orders: 'po_id',
    PO_Line_Items: 'line_id',
    Audit_Trail: 'audit_id'
  },
  // ID prefixes
  PREFIXES: {
    SKU_Metadata: 'SKU',
    Inventory_Master: 'INV',
    Scan_Logs: 'TXN',
    Purchase_Orders: 'PO',
    PO_Line_Items: 'POL',
    Audit_Trail: 'AUD'
  }
};

// =============================================================================
// SECTION A: DATA ACCESS LAYER (DAL) — The Snowflake-swappable wrappers
// These are the ONLY functions that touch SpreadsheetApp directly.
// =============================================================================

/**
 * Get the spreadsheet instance (cached per execution)
 */
function getSpreadsheet_() {
  return SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
}

/**
 * Get a sheet by name
 */
function getSheet_(sheetName) {
  const sheet = getSpreadsheet_().getSheetByName(sheetName);
  if (!sheet) throw new Error(`Sheet "${sheetName}" not found`);
  return sheet;
}

/**
 * Get column headers for a sheet
 * @param {string} sheetName
 * @returns {string[]} Array of header strings
 */
function getSheetHeaders(sheetName) {
  const sheet = getSheet_(sheetName);
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  return headers.map(h => String(h).trim());
}

/**
 * Generate a unique ID with prefix
 * Format: PREFIX-YYYYMMDD-HHMMSS-XXXX (4 random hex chars)
 * @param {string} prefix - e.g. 'SKU', 'TXN', 'PO'
 * @returns {string}
 */
function generateId(prefix) {
  const now = new Date();
  const ts = Utilities.formatDate(now, 'America/New_York', 'yyyyMMdd-HHmmss');
  const rand = Math.random().toString(16).substring(2, 6);
  return `${prefix}-${ts}-${rand}`;
}

/**
 * Read all data from a sheet, optionally filtered
 * @param {string} sheetName
 * @param {Object} [filters] - Optional { columnName: value } pairs for WHERE-like filtering
 * @returns {Object[]} Array of row objects keyed by headers
 */
function getData(sheetName, filters) {
  const sheet = getSheet_(sheetName);
  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return []; // Only headers, no data

  const headers = getSheetHeaders(sheetName);
  const dataRange = sheet.getRange(2, 1, lastRow - 1, headers.length);
  const rows = dataRange.getValues();

  let results = rows.map(row => {
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = row[i];
    });
    return obj;
  });

  // Apply filters if provided
  if (filters && typeof filters === 'object') {
    const filterKeys = Object.keys(filters);
    results = results.filter(row => {
      return filterKeys.every(key => String(row[key]) === String(filters[key]));
    });
  }

  return results;
}

/**
 * Append a new row to a sheet
 * @param {string} sheetName
 * @param {Object} rowObject - Keys must match header names
 * @returns {string} The generated primary key
 */
function setData(sheetName, rowObject) {
  const sheet = getSheet_(sheetName);
  const headers = getSheetHeaders(sheetName);
  const pkColumn = CONFIG.PRIMARY_KEYS[sheetName];
  const prefix = CONFIG.PREFIXES[sheetName];

  // Auto-generate PK if not provided
  if (pkColumn && !rowObject[pkColumn]) {
    rowObject[pkColumn] = generateId(prefix);
  }

  // Build row array matching header order
  const rowArray = headers.map(header => {
    return rowObject[header] !== undefined ? rowObject[header] : '';
  });

  sheet.appendRow(rowArray);
  return rowObject[pkColumn] || '';
}

/**
 * Update an existing row by primary key
 * @param {string} sheetName
 * @param {string} primaryKey - The PK value to find
 * @param {Object} updates - { columnName: newValue } pairs to update
 * @returns {boolean} True if row was found and updated
 */
function updateData(sheetName, primaryKey, updates) {
  const sheet = getSheet_(sheetName);
  const headers = getSheetHeaders(sheetName);
  const pkColumn = CONFIG.PRIMARY_KEYS[sheetName];
  const pkIndex = headers.indexOf(pkColumn);

  if (pkIndex === -1) throw new Error(`Primary key column "${pkColumn}" not found in ${sheetName}`);

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return false;

  // Find the row with matching PK
  const pkValues = sheet.getRange(2, pkIndex + 1, lastRow - 1, 1).getValues();
  let targetRowIndex = -1;

  for (let i = 0; i < pkValues.length; i++) {
    if (String(pkValues[i][0]) === String(primaryKey)) {
      targetRowIndex = i + 2; // +2 for 1-indexed and header row
      break;
    }
  }

  if (targetRowIndex === -1) return false;

  // Apply updates
  const updateKeys = Object.keys(updates);
  updateKeys.forEach(key => {
    const colIndex = headers.indexOf(key);
    if (colIndex !== -1) {
      sheet.getRange(targetRowIndex, colIndex + 1).setValue(updates[key]);
    }
  });

  return true;
}

/**
 * Soft-delete a row (sets is_active=false or status=CANCELLED)
 * @param {string} sheetName
 * @param {string} primaryKey
 * @returns {boolean}
 */
function deleteData(sheetName, primaryKey) {
  const headers = getSheetHeaders(sheetName);

  if (headers.includes('is_active')) {
    return updateData(sheetName, primaryKey, { is_active: false });
  } else if (headers.includes('status')) {
    return updateData(sheetName, primaryKey, { status: 'CANCELLED' });
  }

  throw new Error(`Sheet "${sheetName}" has no soft-delete column (is_active or status)`);
}

/**
 * Get a single row by primary key
 * @param {string} sheetName
 * @param {string} pkValue
 * @returns {Object|null}
 */
function getRowByPK(sheetName, pkValue) {
  const pkColumn = CONFIG.PRIMARY_KEYS[sheetName];
  const results = getData(sheetName, { [pkColumn]: pkValue });
  return results.length > 0 ? results[0] : null;
}


// =============================================================================
// SECTION B: SKU & BARCODE RESOLUTION
// =============================================================================

/**
 * Look up a SKU by barcode value (checks both vendor and internal barcodes)
 * @param {string} barcodeValue - Raw barcode string from scanner
 * @returns {Object|null} Full SKU row or null
 */
function lookupSKUByBarcode(barcodeValue) {
  const allSKUs = getData(CONFIG.SHEETS.SKU_METADATA);
  const match = allSKUs.find(sku =>
    (String(sku.barcode_value) === String(barcodeValue) ||
     String(sku.internal_barcode) === String(barcodeValue)) &&
    sku.is_active !== false && sku.is_active !== 'FALSE'
  );
  return match || null;
}

/**
 * Look up a SKU by its ID
 * @param {string} skuId
 * @returns {Object|null}
 */
function lookupSKUById(skuId) {
  return getRowByPK(CONFIG.SHEETS.SKU_METADATA, skuId);
}

/**
 * Create a new SKU entry
 * @param {Object} skuData - SKU fields (vendor_sku, sku_name, category, etc.)
 * @returns {string} Generated sku_id
 */
function createSKU(skuData) {
  const now = new Date().toISOString();
  skuData.is_active = true;
  skuData.created_at = now;
  skuData.updated_at = now;

  // Calculate case_price if not provided
  if (!skuData.case_price && skuData.units_per_case && skuData.price_per_unit) {
    skuData.case_price = Number(skuData.units_per_case) * Number(skuData.price_per_unit);
  }

  const skuId = setData(CONFIG.SHEETS.SKU_METADATA, skuData);

  // Also create an Inventory_Master row for this SKU
  setData(CONFIG.SHEETS.INVENTORY_MASTER, {
    sku_id: skuId,
    total_available: 0,
    sealed_cases: 0,
    loose_units: 0,
    total_value: 0,
    stock_status: 'OUT_OF_STOCK',
    updated_at: now
  });

  // Audit trail
  writeAuditEntry({
    operator_id: 'SYSTEM',
    action: 'CREATE',
    target_sheet: CONFIG.SHEETS.SKU_METADATA,
    target_row_id: skuId,
    new_value: JSON.stringify(skuData),
    notes: 'New SKU created'
  });

  return skuId;
}

/**
 * Update an existing SKU
 * @param {string} skuId
 * @param {Object} updates
 * @returns {boolean}
 */
function updateSKU(skuId, updates) {
  const oldData = lookupSKUById(skuId);
  if (!oldData) return false;

  updates.updated_at = new Date().toISOString();
  const success = updateData(CONFIG.SHEETS.SKU_METADATA, skuId, updates);

  if (success) {
    Object.keys(updates).forEach(field => {
      if (field !== 'updated_at') {
        writeAuditEntry({
          operator_id: 'SYSTEM',
          action: 'UPDATE',
          target_sheet: CONFIG.SHEETS.SKU_METADATA,
          target_row_id: skuId,
          field_changed: field,
          old_value: String(oldData[field] || ''),
          new_value: String(updates[field]),
          notes: 'SKU updated'
        });
      }
    });
  }

  return success;
}

/**
 * Assign a new internal barcode to a SKU (relabeling module)
 * @param {string} skuId
 * @param {string} newBarcodeValue
 * @returns {boolean}
 */
function assignInternalBarcode(skuId, newBarcodeValue) {
  return updateSKU(skuId, { internal_barcode: newBarcodeValue });
}


// =============================================================================
// SECTION C: INVENTORY CRUD
// =============================================================================

/**
 * Get inventory record for a specific SKU
 * @param {string} skuId
 * @returns {Object|null}
 */
function getInventoryForSKU(skuId) {
  const results = getData(CONFIG.SHEETS.INVENTORY_MASTER, { sku_id: skuId });
  return results.length > 0 ? results[0] : null;
}

/**
 * Adjust inventory quantities for a SKU
 * @param {string} skuId
 * @param {number} quantityDelta - Positive to add, negative to subtract
 * @param {string} reason - Why the adjustment is happening
 * @param {string} operatorId - Who performed the action
 * @param {Object} [caseAdjustment] - Optional { sealed_cases_delta, loose_units_delta }
 * @returns {Object} Updated inventory row
 */
function adjustInventory(skuId, quantityDelta, reason, operatorId, caseAdjustment) {
  const inventory = getInventoryForSKU(skuId);
  if (!inventory) {
    throw new Error(`No inventory record found for SKU: ${skuId}`);
  }

  const sku = lookupSKUById(skuId);
  const unitsPerCase = sku ? Number(sku.units_per_case) || 1 : 1;
  const pricePerUnit = sku ? Number(sku.price_per_unit) || 0 : 0;

  const oldTotal = Number(inventory.total_available) || 0;
  const oldSealed = Number(inventory.sealed_cases) || 0;
  const oldLoose = Number(inventory.loose_units) || 0;

  let newSealed = oldSealed;
  let newLoose = oldLoose;

  if (caseAdjustment) {
    newSealed += (caseAdjustment.sealed_cases_delta || 0);
    newLoose += (caseAdjustment.loose_units_delta || 0);
  } else {
    // Default: if adding full cases worth, increment sealed; otherwise loose
    if (quantityDelta > 0 && quantityDelta % unitsPerCase === 0 && unitsPerCase > 1) {
      newSealed += quantityDelta / unitsPerCase;
    } else {
      newLoose += quantityDelta;
    }
  }

  // Ensure non-negative
  newSealed = Math.max(0, newSealed);
  newLoose = Math.max(0, newLoose);

  const newTotal = (newSealed * unitsPerCase) + newLoose;
  const newValue = newTotal * pricePerUnit;

  // Determine stock status
  const reorderThreshold = sku ? Number(sku.reorder_threshold) || 0 : 0;
  let stockStatus = 'IN_STOCK';
  if (newTotal <= 0) stockStatus = 'OUT_OF_STOCK';
  else if (newTotal <= reorderThreshold) stockStatus = 'LOW_STOCK';

  const now = new Date().toISOString();
  const updates = {
    total_available: newTotal,
    sealed_cases: newSealed,
    loose_units: newLoose,
    total_value: newValue,
    stock_status: stockStatus,
    updated_at: now
  };

  // Set last_intake_date if this is an intake
  if (quantityDelta > 0 && (reason === 'INTAKE' || reason === 'RETURN_TO_STOCK')) {
    updates.last_intake_date = now;
  }

  updateData(CONFIG.SHEETS.INVENTORY_MASTER, inventory.inventory_id, updates);

  // Audit trail
  writeAuditEntry({
    operator_id: operatorId,
    action: 'ADJUST',
    target_sheet: CONFIG.SHEETS.INVENTORY_MASTER,
    target_row_id: inventory.inventory_id,
    field_changed: 'total_available',
    old_value: String(oldTotal),
    new_value: String(newTotal),
    notes: `${reason}: delta=${quantityDelta}`
  });

  return { ...inventory, ...updates };
}

/**
 * Recalculate stock status for a SKU
 * @param {string} skuId
 */
function recalculateStockStatus(skuId) {
  const inventory = getInventoryForSKU(skuId);
  const sku = lookupSKUById(skuId);
  if (!inventory || !sku) return;

  const total = Number(inventory.total_available) || 0;
  const threshold = Number(sku.reorder_threshold) || 0;

  let status = 'IN_STOCK';
  if (total <= 0) status = 'OUT_OF_STOCK';
  else if (total <= threshold) status = 'LOW_STOCK';

  updateData(CONFIG.SHEETS.INVENTORY_MASTER, inventory.inventory_id, {
    stock_status: status,
    updated_at: new Date().toISOString()
  });
}

/**
 * Get full inventory summary joined with SKU metadata (for dashboard)
 * @returns {Object[]}
 */
function getInventorySummary() {
  const inventory = getData(CONFIG.SHEETS.INVENTORY_MASTER);
  const skus = getData(CONFIG.SHEETS.SKU_METADATA);

  const skuMap = {};
  skus.forEach(sku => { skuMap[sku.sku_id] = sku; });

  return inventory.map(inv => {
    const sku = skuMap[inv.sku_id] || {};
    return {
      ...inv,
      sku_name: sku.sku_name || 'Unknown',
      category: sku.category || '',
      vendor_name: sku.vendor_name || '',
      price_per_unit: sku.price_per_unit || 0,
      units_per_case: sku.units_per_case || 0,
      reorder_threshold: sku.reorder_threshold || 0,
      avg_weekly_qty_4wk: sku.avg_weekly_qty_4wk || 0
    };
  });
}

/**
 * Get items with low or zero stock
 * @returns {Object[]}
 */
function getLowStockItems() {
  return getInventorySummary().filter(item =>
    item.stock_status === 'LOW_STOCK' || item.stock_status === 'OUT_OF_STOCK'
  );
}

/**
 * Get total value of all inventory on hand
 * @returns {number}
 */
function getTotalInventoryValue() {
  const inventory = getData(CONFIG.SHEETS.INVENTORY_MASTER);
  return inventory.reduce((sum, row) => sum + (Number(row.total_value) || 0), 0);
}


// =============================================================================
// SECTION D: SCAN PROCESSING & DOUBLE-COUNT PREVENTION
// =============================================================================

/**
 * Main entry point for processing a barcode scan from the frontend
 * @param {Object} scanPayload
 * @param {string} scanPayload.barcodeValue - Raw barcode string
 * @param {string} scanPayload.scanType - INTAKE | RETURN_TO_STOCK | PHYSICAL_COUNT | BREAK_CASE
 * @param {string} scanPayload.operatorId - User email/name
 * @param {string} scanPayload.sessionId - Current session ID
 * @param {string} [scanPayload.poId] - Purchase Order ID (for INTAKE)
 * @param {number} [scanPayload.quantity] - Manual quantity (for BREAK_CASE or manual count)
 * @param {string} [scanPayload.notes] - Optional notes
 * @returns {Object} { success, transaction, message, skuData, inventoryData }
 */
function processScan(scanPayload) {
  try {
    const { barcodeValue, scanType, operatorId, sessionId, poId, quantity, notes } = scanPayload;

    // 1. Resolve barcode to SKU
    const sku = lookupSKUByBarcode(barcodeValue);
    if (!sku) {
      const txn = logScanTransaction({
        barcode_scanned: barcodeValue,
        scan_type: scanType,
        operator_id: operatorId,
        session_id: sessionId,
        status: 'REJECTED_NO_SKU',
        rejection_reason: 'Barcode not found in SKU database',
        notes: notes
      });
      return {
        success: false,
        transaction: txn,
        message: `Barcode "${barcodeValue}" not found. Please register this SKU first.`
      };
    }

    // 2. Check for duplicate scan (session-scoped)
    const dupCheck = checkDuplicateScan(barcodeValue, sessionId);
    if (dupCheck.isDuplicate) {
      const txn = logScanTransaction({
        barcode_scanned: barcodeValue,
        sku_id: sku.sku_id,
        scan_type: scanType,
        operator_id: operatorId,
        session_id: sessionId,
        status: 'REJECTED_DUPLICATE',
        rejection_reason: `Already scanned in this session (${dupCheck.existingTransaction.transaction_id})`,
        notes: notes
      });
      return {
        success: false,
        transaction: txn,
        message: `DUPLICATE SCAN: This barcode was already scanned in this session.`,
        skuData: sku
      };
    }

    // 3. Branch on scan type
    let result;
    switch (scanType) {
      case 'INTAKE':
        result = processIntake_(sku, scanPayload);
        break;
      case 'RETURN_TO_STOCK':
        result = processReturnToStock_(sku, scanPayload);
        break;
      case 'PHYSICAL_COUNT':
        result = processPhysicalCount_(sku, scanPayload);
        break;
      case 'BREAK_CASE':
        result = processBreakCase(scanPayload.parentTransactionId, quantity, operatorId);
        return result; // Break case has its own return format
      default:
        throw new Error(`Unknown scan type: ${scanType}`);
    }

    return result;

  } catch (error) {
    Logger.log(`processScan error: ${error.message}`);
    return {
      success: false,
      message: `Error processing scan: ${error.message}`
    };
  }
}

/**
 * Process an INTAKE scan (receiving against a PO)
 * @private
 */
function processIntake_(sku, scanPayload) {
  const { barcodeValue, operatorId, sessionId, poId, notes } = scanPayload;
  const unitsPerCase = Number(sku.units_per_case) || 1;

  // Validate against PO if provided
  if (poId) {
    const poValidation = validateAgainstPO(poId, sku.sku_id, unitsPerCase);
    if (!poValidation.valid) {
      const txn = logScanTransaction({
        barcode_scanned: barcodeValue,
        sku_id: sku.sku_id,
        scan_type: 'INTAKE',
        quantity: unitsPerCase,
        case_status: 'SEALED_CASE',
        po_id: poId,
        operator_id: operatorId,
        session_id: sessionId,
        status: 'REJECTED_NO_PO',
        rejection_reason: poValidation.message,
        notes: notes
      });
      return {
        success: false,
        transaction: txn,
        message: poValidation.message,
        skuData: sku
      };
    }
  }

  // Adjust inventory (add full case)
  const updatedInventory = adjustInventory(
    sku.sku_id,
    unitsPerCase,
    'INTAKE',
    operatorId,
    { sealed_cases_delta: 1, loose_units_delta: 0 }
  );

  // Update PO line item if applicable
  if (poId) {
    updatePOLineItem(poId, sku.sku_id, unitsPerCase);
  }

  // Log successful transaction
  const txn = logScanTransaction({
    barcode_scanned: barcodeValue,
    sku_id: sku.sku_id,
    scan_type: 'INTAKE',
    quantity: unitsPerCase,
    case_status: 'SEALED_CASE',
    po_id: poId || '',
    operator_id: operatorId,
    session_id: sessionId,
    status: 'ACCEPTED',
    notes: notes
  });

  return {
    success: true,
    transaction: txn,
    message: `Intake successful: ${sku.sku_name} — ${unitsPerCase} units (1 sealed case)`,
    skuData: sku,
    inventoryData: updatedInventory
  };
}

/**
 * Process a RETURN_TO_STOCK scan
 * @private
 */
function processReturnToStock_(sku, scanPayload) {
  const { barcodeValue, operatorId, sessionId, quantity, notes } = scanPayload;
  const qty = Number(quantity) || Number(sku.units_per_case) || 1;

  const updatedInventory = adjustInventory(sku.sku_id, qty, 'RETURN_TO_STOCK', operatorId);

  const txn = logScanTransaction({
    barcode_scanned: barcodeValue,
    sku_id: sku.sku_id,
    scan_type: 'RETURN_TO_STOCK',
    quantity: qty,
    case_status: 'N/A',
    operator_id: operatorId,
    session_id: sessionId,
    status: 'ACCEPTED',
    notes: notes
  });

  return {
    success: true,
    transaction: txn,
    message: `Returned to stock: ${sku.sku_name} — ${qty} units`,
    skuData: sku,
    inventoryData: updatedInventory
  };
}

/**
 * Process a PHYSICAL_COUNT scan
 * @private
 */
function processPhysicalCount_(sku, scanPayload) {
  const { barcodeValue, operatorId, sessionId, quantity, notes } = scanPayload;

  const txn = logScanTransaction({
    barcode_scanned: barcodeValue,
    sku_id: sku.sku_id,
    scan_type: 'PHYSICAL_COUNT',
    quantity: Number(quantity) || 0,
    case_status: 'N/A',
    operator_id: operatorId,
    session_id: sessionId,
    status: 'ACCEPTED',
    notes: notes
  });

  // Update last_count_date
  const inventory = getInventoryForSKU(sku.sku_id);
  if (inventory) {
    updateData(CONFIG.SHEETS.INVENTORY_MASTER, inventory.inventory_id, {
      last_count_date: new Date().toISOString(),
      updated_at: new Date().toISOString()
    });
  }

  return {
    success: true,
    transaction: txn,
    message: `Physical count recorded: ${sku.sku_name} — ${quantity} units`,
    skuData: sku
  };
}

/**
 * Check if a barcode has already been scanned in the current session
 * @param {string} barcodeValue
 * @param {string} sessionId
 * @returns {Object} { isDuplicate: boolean, existingTransaction?: Object }
 */
function checkDuplicateScan(barcodeValue, sessionId) {
  const logs = getData(CONFIG.SHEETS.SCAN_LOGS);
  const existing = logs.find(log =>
    String(log.barcode_scanned) === String(barcodeValue) &&
    String(log.session_id) === String(sessionId) &&
    log.status === 'ACCEPTED'
  );

  return {
    isDuplicate: !!existing,
    existingTransaction: existing || null
  };
}

/**
 * Log a scan transaction to Scan_Logs
 * @param {Object} transactionData
 * @returns {Object} The logged transaction with generated ID
 */
function logScanTransaction(transactionData) {
  transactionData.scan_timestamp = new Date().toISOString();
  const txnId = setData(CONFIG.SHEETS.SCAN_LOGS, transactionData);
  transactionData.transaction_id = txnId;
  return transactionData;
}


// =============================================================================
// SECTION E: PARENT/CHILD CASE LOGIC
// =============================================================================

/**
 * Process breaking open a sealed case
 * @param {string} parentTransactionId - Original sealed case transaction ID
 * @param {number} remainingUnits - Physical count of remaining individual units
 * @param {string} operatorId
 * @returns {Object} { success, updatedInventory, transaction }
 */
function processBreakCase(parentTransactionId, remainingUnits, operatorId) {
  try {
    // 1. Find the original sealed-case transaction
    const parentTxn = getRowByPK(CONFIG.SHEETS.SCAN_LOGS, parentTransactionId);
    if (!parentTxn) {
      return { success: false, message: `Parent transaction "${parentTransactionId}" not found` };
    }

    // 2. Get SKU info
    const sku = lookupSKUById(parentTxn.sku_id);
    if (!sku) {
      return { success: false, message: `SKU not found for parent transaction` };
    }

    const unitsPerCase = Number(sku.units_per_case) || 1;
    remainingUnits = Number(remainingUnits) || 0;

    // 3. Adjust inventory: remove 1 sealed case, add remaining as loose units
    // Net change: -(unitsPerCase) + remainingUnits = -(unitsPerCase - remainingUnits)
    const updatedInventory = adjustInventory(
      sku.sku_id,
      -(unitsPerCase - remainingUnits), // net delta
      'BREAK_CASE',
      operatorId,
      { sealed_cases_delta: -1, loose_units_delta: remainingUnits }
    );

    // 4. Log the break-case transaction
    const txn = logScanTransaction({
      barcode_scanned: parentTxn.barcode_scanned,
      sku_id: sku.sku_id,
      scan_type: 'BREAK_CASE',
      quantity: remainingUnits,
      case_status: 'OPEN_CASE',
      parent_transaction_id: parentTransactionId,
      operator_id: operatorId,
      session_id: parentTxn.session_id,
      status: 'ACCEPTED',
      notes: `Broke case: ${unitsPerCase} full → ${remainingUnits} remaining`
    });

    return {
      success: true,
      transaction: txn,
      message: `Case broken: ${sku.sku_name} — ${remainingUnits} of ${unitsPerCase} units remaining`,
      skuData: sku,
      inventoryData: updatedInventory
    };

  } catch (error) {
    return { success: false, message: `Error breaking case: ${error.message}` };
  }
}

/**
 * Determine if a scanned barcode represents a sealed case or individual unit
 * @param {string} skuId
 * @param {string} barcodeValue
 * @returns {string} 'SEALED_CASE' or 'INDIVIDUAL'
 */
function determineCaseStatus(skuId, barcodeValue) {
  const sku = lookupSKUById(skuId);
  if (!sku) return 'INDIVIDUAL';

  const unitsPerCase = Number(sku.units_per_case) || 1;
  if (unitsPerCase > 1 && String(sku.barcode_value) === String(barcodeValue)) {
    return 'SEALED_CASE';
  }
  return 'INDIVIDUAL';
}


// =============================================================================
// SECTION F: PURCHASE ORDER MANAGEMENT
// =============================================================================

/**
 * Create a new Purchase Order with line items
 * @param {Object} poData
 * @param {string} poData.vendorName
 * @param {string} poData.orderDate
 * @param {string} poData.expectedDeliveryDate
 * @param {Object[]} poData.lineItems - Array of { skuId, orderedQty, unitCost }
 * @param {string} poData.createdBy
 * @param {string} [poData.notes]
 * @returns {string} Generated po_id
 */
function createPurchaseOrder(poData) {
  const now = new Date().toISOString();

  const poId = setData(CONFIG.SHEETS.PURCHASE_ORDERS, {
    vendor_name: poData.vendorName,
    order_date: poData.orderDate,
    expected_delivery_date: poData.expectedDeliveryDate,
    status: 'SUBMITTED',
    created_by: poData.createdBy,
    notes: poData.notes || '',
    created_at: now,
    updated_at: now
  });

  // Create line items
  if (poData.lineItems && Array.isArray(poData.lineItems)) {
    poData.lineItems.forEach(item => {
      setData(CONFIG.SHEETS.PO_LINE_ITEMS, {
        po_id: poId,
        sku_id: item.skuId,
        ordered_qty: item.orderedQty,
        received_qty: 0,
        unit_cost: item.unitCost || 0,
        line_status: 'PENDING',
        updated_at: now
      });
    });
  }

  writeAuditEntry({
    operator_id: poData.createdBy,
    action: 'CREATE',
    target_sheet: CONFIG.SHEETS.PURCHASE_ORDERS,
    target_row_id: poId,
    new_value: JSON.stringify(poData),
    notes: 'Purchase order created'
  });

  return poId;
}

/**
 * Get a Purchase Order with its line items
 * @param {string} poId
 * @returns {Object|null} PO header + lineItems array
 */
function getPurchaseOrder(poId) {
  const po = getRowByPK(CONFIG.SHEETS.PURCHASE_ORDERS, poId);
  if (!po) return null;

  const lineItems = getData(CONFIG.SHEETS.PO_LINE_ITEMS, { po_id: poId });
  po.lineItems = lineItems;
  return po;
}

/**
 * Recalculate PO status based on line item statuses
 * @param {string} poId
 */
function updatePOStatus(poId) {
  const lineItems = getData(CONFIG.SHEETS.PO_LINE_ITEMS, { po_id: poId });
  if (lineItems.length === 0) return;

  const allComplete = lineItems.every(li => li.line_status === 'COMPLETE' || li.line_status === 'OVER_RECEIVED');
  const anyReceived = lineItems.some(li => Number(li.received_qty) > 0);

  let status = 'SUBMITTED';
  if (allComplete) status = 'FULLY_RECEIVED';
  else if (anyReceived) status = 'PARTIALLY_RECEIVED';

  updateData(CONFIG.SHEETS.PURCHASE_ORDERS, poId, {
    status: status,
    updated_at: new Date().toISOString()
  });
}

/**
 * Validate a scan against a Purchase Order
 * @param {string} poId
 * @param {string} skuId
 * @param {number} quantity
 * @returns {Object} { valid, lineItem, message }
 */
function validateAgainstPO(poId, skuId, quantity) {
  const lineItems = getData(CONFIG.SHEETS.PO_LINE_ITEMS, { po_id: poId });
  const matchingLine = lineItems.find(li => String(li.sku_id) === String(skuId));

  if (!matchingLine) {
    return { valid: false, lineItem: null, message: `SKU ${skuId} is not on PO ${poId}` };
  }

  const ordered = Number(matchingLine.ordered_qty) || 0;
  const received = Number(matchingLine.received_qty) || 0;
  const remaining = ordered - received;

  if (remaining <= 0) {
    return {
      valid: false,
      lineItem: matchingLine,
      message: `PO ${poId}: SKU ${skuId} already fully received (${received}/${ordered})`
    };
  }

  if (quantity > remaining) {
    return {
      valid: false,
      lineItem: matchingLine,
      message: `PO ${poId}: Would over-receive SKU ${skuId} (${received + quantity}/${ordered})`
    };
  }

  return { valid: true, lineItem: matchingLine, message: 'OK' };
}

/**
 * Update PO line item received quantity after intake
 * @param {string} poId
 * @param {string} skuId
 * @param {number} additionalQty
 */
function updatePOLineItem(poId, skuId, additionalQty) {
  const lineItems = getData(CONFIG.SHEETS.PO_LINE_ITEMS, { po_id: poId });
  const matchingLine = lineItems.find(li => String(li.sku_id) === String(skuId));
  if (!matchingLine) return;

  const newReceived = (Number(matchingLine.received_qty) || 0) + Number(additionalQty);
  const ordered = Number(matchingLine.ordered_qty) || 0;

  let lineStatus = 'PENDING';
  if (newReceived >= ordered) lineStatus = newReceived > ordered ? 'OVER_RECEIVED' : 'COMPLETE';
  else if (newReceived > 0) lineStatus = 'PARTIAL';

  updateData(CONFIG.SHEETS.PO_LINE_ITEMS, matchingLine.line_id, {
    received_qty: newReceived,
    line_status: lineStatus,
    updated_at: new Date().toISOString()
  });

  // Recalculate PO header status
  updatePOStatus(poId);
}

/**
 * Get all open (non-completed) Purchase Orders
 * @returns {Object[]}
 */
function getOpenPurchaseOrders() {
  const allPOs = getData(CONFIG.SHEETS.PURCHASE_ORDERS);
  return allPOs.filter(po =>
    po.status === 'SUBMITTED' || po.status === 'PARTIALLY_RECEIVED'
  );
}


// =============================================================================
// SECTION G: AUDIT TRAIL
// =============================================================================

/**
 * Write an entry to the Audit_Trail sheet
 * @param {Object} auditData
 * @param {string} auditData.operator_id
 * @param {string} auditData.action - CREATE | UPDATE | DELETE | INTAKE | RETURN | BREAK_CASE | ADJUST
 * @param {string} auditData.target_sheet
 * @param {string} auditData.target_row_id
 * @param {string} [auditData.field_changed]
 * @param {string} [auditData.old_value]
 * @param {string} [auditData.new_value]
 * @param {string} [auditData.transaction_id]
 * @param {string} [auditData.notes]
 */
function writeAuditEntry(auditData) {
  auditData.timestamp = new Date().toISOString();
  setData(CONFIG.SHEETS.AUDIT_TRAIL, auditData);
}

/**
 * Get audit history for a specific record
 * @param {string} targetSheet
 * @param {string} targetRowId
 * @returns {Object[]}
 */
function getAuditHistory(targetSheet, targetRowId) {
  return getData(CONFIG.SHEETS.AUDIT_TRAIL, {
    target_sheet: targetSheet,
    target_row_id: targetRowId
  });
}


// =============================================================================
// SECTION H: PLACEHOLDER WEBHOOKS & NOTIFICATIONS
// =============================================================================

/**
 * Send a Slack notification (stub — wire up webhook URL when ready)
 * @param {Object} payload - { channel, message, severity }
 */
function sendSlackNotification(payload) {
  Logger.log(`[SLACK STUB] Channel: ${payload.channel}, Message: ${payload.message}`);
  // Future implementation:
  // const SLACK_WEBHOOK_URL = PropertiesService.getScriptProperties().getProperty('SLACK_WEBHOOK');
  // UrlFetchApp.fetch(SLACK_WEBHOOK_URL, {
  //   method: 'post',
  //   contentType: 'application/json',
  //   payload: JSON.stringify({ text: payload.message, channel: payload.channel })
  // });
}

/**
 * Send a WhatsApp alert (stub — wire up Twilio/WhatsApp API when ready)
 * @param {Object} payload - { recipientPhone, message }
 */
function sendWhatsAppAlert(payload) {
  Logger.log(`[WHATSAPP STUB] To: ${payload.recipientPhone}, Message: ${payload.message}`);
  // Future implementation:
  // Use UrlFetchApp with Twilio API
}

/**
 * Check for low stock and send alerts
 * Designed to be called by a time-driven trigger (daily/hourly)
 */
function checkAndAlertLowStock() {
  const lowStockItems = getLowStockItems();

  if (lowStockItems.length === 0) {
    Logger.log('No low stock items detected.');
    return;
  }

  const alertLines = lowStockItems.map(item =>
    `⚠️ ${item.sku_name}: ${item.total_available} units (threshold: ${item.reorder_threshold})`
  );

  const message = `LOW STOCK ALERT\n${alertLines.join('\n')}`;

  sendSlackNotification({
    channel: '#inventory-alerts',
    message: message,
    severity: 'warning'
  });

  sendWhatsAppAlert({
    recipientPhone: '', // Configure when ready
    message: message
  });

  Logger.log(`Low stock alert sent for ${lowStockItems.length} items`);
}


// =============================================================================
// SECTION I: WEB APP ENTRY POINTS
// =============================================================================

/**
 * Required GAS web app entry point
 * Serves the main HTML page optimized for tablets
 */
function doGet(e) {
  // Route to different pages based on ?page= parameter
  const page = (e && e.parameter && e.parameter.page) || 'Index';
  const validPages = ['Index', 'LabelGenerator'];
  const pageName = validPages.includes(page) ? page : 'Index';

  const template = HtmlService.createTemplateFromFile(pageName);
  return template.evaluate()
    .setTitle('Cannabis Packaging Inventory')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

/**
 * Include HTML partials (CSS, JS files) in the main template
 * Usage in HTML: <?!= include('Stylesheet'); ?>
 * @param {string} filename
 * @returns {string} Raw HTML content
 */
function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}


// =============================================================================
// SECTION J: UTILITY / BOOTSTRAP
// =============================================================================

/**
 * Generate a new session ID for a scanning session
 * @returns {string}
 */
function generateSessionId() {
  return generateId('SES');
}

// =============================================================================
// SECTION K: QR LABEL GENERATION & SERIAL MANAGEMENT
// =============================================================================

/**
 * Get all SKUs for the label generator UI
 * @returns {Object[]} Array of SKU objects
 */
function getAllSKUs() {
  return getData(CONFIG.SHEETS.SKU_METADATA);
}

/**
 * Generate unique serial codes for labels and register them in the database.
 * Each serial is globally unique and tied to a specific SKU.
 *
 * Serial format: {SKU_ID}:{5-digit-sequence}-{4-hex-random}
 * Example: HC-34-15-I:00001-a7f2
 *
 * The serial encodes:
 *   - The SKU identity (so scanning resolves the product)
 *   - A unique suffix (so no two physical labels share a code)
 *
 * @param {string} skuId - The SKU to generate labels for
 * @param {number} quantity - How many unique labels to generate
 * @returns {Object[]} Array of { serialCode, skuId, sequenceNumber }
 */
function generateLabelSerials(skuId, quantity) {
  const sku = lookupSKUById(skuId);
  if (!sku) throw new Error(`SKU "${skuId}" not found`);

  quantity = Math.min(Math.max(1, Number(quantity) || 1), 5000);

  // Find the highest existing sequence number for this SKU
  const existingLogs = getData(CONFIG.SHEETS.SCAN_LOGS);
  let maxSeq = 0;
  const prefix = skuId + ':';

  existingLogs.forEach(log => {
    const bc = String(log.barcode_scanned || '');
    if (bc.startsWith(prefix)) {
      const seqPart = bc.substring(prefix.length).split('-')[0];
      const seq = parseInt(seqPart, 10);
      if (!isNaN(seq) && seq > maxSeq) maxSeq = seq;
    }
  });

  // Also check previously generated labels (in case they haven't been scanned yet)
  // We store generated serials in a dedicated sheet or in Scan_Logs with type LABEL_GENERATED
  const labelLogs = existingLogs.filter(log => log.scan_type === 'LABEL_GENERATED');
  labelLogs.forEach(log => {
    const bc = String(log.barcode_scanned || '');
    if (bc.startsWith(prefix)) {
      const seqPart = bc.substring(prefix.length).split('-')[0];
      const seq = parseInt(seqPart, 10);
      if (!isNaN(seq) && seq > maxSeq) maxSeq = seq;
    }
  });

  const serials = [];
  const now = new Date().toISOString();
  const batchId = generateId('LBL');

  for (let i = 0; i < quantity; i++) {
    const seq = maxSeq + i + 1;
    const seqStr = String(seq).padStart(5, '0');
    const rand = Math.random().toString(16).substring(2, 6);
    const serialCode = `${skuId}:${seqStr}-${rand}`;

    serials.push({
      serialCode: serialCode,
      skuId: skuId,
      sequenceNumber: seq
    });

    // Register each serial in Scan_Logs so it's trackable
    setData(CONFIG.SHEETS.SCAN_LOGS, {
      scan_timestamp: now,
      barcode_scanned: serialCode,
      sku_id: skuId,
      scan_type: 'LABEL_GENERATED',
      quantity: 1,
      case_status: 'N/A',
      operator_id: 'SYSTEM',
      session_id: batchId,
      status: 'PENDING',
      notes: `Label generated in batch ${batchId}`
    });
  }

  // Audit trail for the batch
  writeAuditEntry({
    operator_id: 'SYSTEM',
    action: 'CREATE',
    target_sheet: CONFIG.SHEETS.SCAN_LOGS,
    target_row_id: batchId,
    new_value: `Generated ${quantity} labels for ${skuId} (seq ${maxSeq + 1}-${maxSeq + quantity})`,
    notes: `Label batch: ${batchId}`
  });

  return serials;
}

/**
 * Resolve a scanned serial code back to its SKU.
 * Handles both plain SKU barcodes and serial-encoded QR codes.
 * @param {string} scannedValue - The raw value from the scanner
 * @returns {Object|null} { skuId, serialCode, isUniqueSeral }
 */
function resolveScannedCode(scannedValue) {
  // Check if it's a serial-encoded QR (contains ':')
  if (scannedValue.includes(':')) {
    const skuId = scannedValue.split(':')[0];
    const sku = lookupSKUById(skuId);
    if (sku) {
      return { skuId: skuId, serialCode: scannedValue, isUniqueSerial: true, sku: sku };
    }
  }

  // Fall back to plain barcode lookup
  const sku = lookupSKUByBarcode(scannedValue);
  if (sku) {
    return { skuId: sku.sku_id, serialCode: scannedValue, isUniqueSerial: false, sku: sku };
  }

  return null;
}

/**
 * Check if a specific serial code has already been used (scanned for intake/count)
 * @param {string} serialCode
 * @returns {Object} { isUsed, transaction }
 */
function checkSerialUsed(serialCode) {
  const logs = getData(CONFIG.SHEETS.SCAN_LOGS);
  const usedTxn = logs.find(log =>
    String(log.barcode_scanned) === String(serialCode) &&
    log.status === 'ACCEPTED' &&
    log.scan_type !== 'LABEL_GENERATED'
  );

  return {
    isUsed: !!usedTxn,
    transaction: usedTxn || null
  };
}


/**
 * Test function — verify database connectivity
 * Run this from the Apps Script editor to confirm everything works
 */
function testDatabaseConnection() {
  try {
    const ss = getSpreadsheet_();
    Logger.log(`Connected to: ${ss.getName()}`);

    Object.values(CONFIG.SHEETS).forEach(sheetName => {
      const headers = getSheetHeaders(sheetName);
      Logger.log(`${sheetName}: ${headers.length} columns — ${headers.join(', ')}`);
    });

    Logger.log('✅ All sheets accessible. Database connection verified.');
    return true;
  } catch (error) {
    Logger.log(`❌ Database connection failed: ${error.message}`);
    return false;
  }
}
