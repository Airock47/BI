const CAN_EDIT = window.PROCURE_CAN_EDIT;

let dataTable = null;
const dirtyMap = new Map(); // id -> {arrival_date, dispatch_date, goods_status, rowData}

function escapeHtml(str) {
    if (str === null || str === undefined) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function showAlert(message, type = 'success') {
    const box = $('#alert-box');
    box.removeClass('d-none alert-success alert-danger').addClass(`alert-${type}`).text(message);
    setTimeout(() => box.addClass('d-none'), 2500);
}

function formatDateValue(value) {
    if (!value) return '';
    const s = String(value);
    if (s.length === 0 || s.toLowerCase() === 'null') return '';
    return s.substring(0, 10);
}

function renderStatusSelect(current, rowId) {
    const list = window.PROCURE_STATUS_OPTIONS || [];
    let found = false;
    let options = list.map(opt => {
        if (opt === current) found = true;
        const selected = opt === current ? 'selected' : '';
        return `<option value="${escapeHtml(opt)}" ${selected}>${escapeHtml(opt)}</option>`;
    }).join('');

    if (current && !found) {
        options += `<option value="${escapeHtml(current)}" selected>${escapeHtml(current)}</option>`;
    }
    const disabled = CAN_EDIT ? '' : 'disabled';
    return `<select class="form-select form-select-sm status-select" data-id="${escapeHtml(rowId)}" ${disabled}>${options}</select>`;
}

function renderArrivalInput(value, rowId, cssClass = 'arrival-input') {
    const v = escapeHtml(formatDateValue(value));
    if (!CAN_EDIT) {
        return `<div class="date-display-text ${cssClass}">${v}</div>`;
    }
    return `<input type="date" class="form-control form-control-sm ${cssClass}" data-id="${escapeHtml(rowId)}" value="${v}">`;
}

function getCategoryFromCode(code) {
    const prefix = (code || '').toString().substring(0, 2);
    if (prefix === '10') return '10';
    if (prefix === '20') return '20';
    if (prefix === '21') return '21';
    if (prefix === '30') return '30';
    return 'other';
}

function renderRemarksInput(value, rowId) {
    const val = escapeHtml(value || '');
    if (!CAN_EDIT) {
        // Use a div with a class for word wrapping
        return `<div class="text-wrap">${val}</div>`;
    }
    // Use textarea for multi-line editing
    return `<textarea class="form-control form-control-sm remarks-input" data-id="${escapeHtml(rowId)}" maxlength="300" rows="1">${val}</textarea>`;
}

function renderTable(data) {
    if (dataTable) {
        dataTable.clear().rows.add(data).draw();
        return;
    }

    dataTable = $('#procureTable').DataTable({
        data,
        responsive: true,
        pageLength: 10,
        order: [[6, 'desc']],
        columns: [
            { data: 'po_number', title: '採購單號', createdCell: (td) => $(td).attr('data-label', '採購單號') },
            { data: 'product_code', title: '產品代碼', createdCell: (td) => $(td).attr('data-label', '產品代碼') },
            { data: 'product_name', title: '商品', createdCell: (td) => $(td).attr('data-label', '商品') },
            { data: 'status', title: 'Excel狀態', createdCell: (td) => $(td).attr('data-label', 'Excel狀態') },
            {
                data: 'goods_status',
                title: '貨物狀態',
                createdCell: (td) => $(td).attr('data-label', '貨物狀態'),
                render: (data, type, row) => {
                    if (type !== 'display') return data || '';
                    return renderStatusSelect(data, row.id);
                }
            },
            {
                data: 'quantity',
                title: '採購數量',
                createdCell: (td) => $(td).attr('data-label', '採購數量'),
                render: (data) => Number(data || 0).toLocaleString()
            },
            {
                data: 'delivery_date',
                title: '交貨日期<br>(系統預計交期)',
                createdCell: (td) => $(td).attr('data-label', '系統預計交期'),
                render: (d) => formatDateValue(d)
            },
            {
                data: 'dispatch_date',
                title: '派送日期<br>(下次交貨日期)',
                createdCell: (td) => $(td).attr('data-label', '下次交貨日期'),
                render: (data, type, row) => {
                    if (type !== 'display') return formatDateValue(data);
                    return renderArrivalInput(data, row.id, 'dispatch-input');
                }
            },
            {
                data: 'arrival_date',
                title: '到港日',
                createdCell: (td) => $(td).attr('data-label', '到港日'),
                render: (data, type, row) => {
                    if (type !== 'display') return formatDateValue(data);
                    return renderArrivalInput(data, row.id, 'arrival-input');
                }
            },
            { data: 'warehouse', title: '倉庫', createdCell: (td) => $(td).attr('data-label', '倉庫') },
            {
                data: 'good_stock',
                title: '良品庫存<br>(不含門市)',
                createdCell: (td) => $(td).attr('data-label', '良品庫存'),
                render: (data, type, row) => {
                    const val = Number(data || 0).toLocaleString();
                    return `<button class="btn btn-link p-0 stock-detail" data-code="${escapeHtml(row.product_code || '')}" data-name="${escapeHtml(row.product_name || '')}">${val}</button>`;
                }
            },
            {
                data: null,
                title: '未交貨數量',
                createdCell: (td) => $(td).attr('data-label', '未交貨數量'),
                render: (data, type, row) => {
                    const q = Number(row.quantity || 0);
                    const w = Number(row.warehouse_qty || 0);
                    return (q - w).toLocaleString();
                }
            },
            {
                data: 'remarks',
                title: '備註',
                createdCell: (td) => $(td).attr('data-label', '備註'),
                render: (data, type, row) => {
                    if (type !== 'display') return data || '';
                    return renderRemarksInput(data, row.id);
                }
            }
        ],
        columnDefs: [
            { targets: 1, visible: false }, // 隱藏產品代碼
            { targets: 3, visible: false }, // 隱藏Excel狀態
            { targets: [0, 4], responsivePriority: 1 },
            { targets: [5, 10, 11], className: 'text-end' },
            { targets: 2, className: 'text-start col-product' },
            { targets: [7, 8], className: 'text-start' }
        ],
        language: {
            url: 'https://cdn.datatables.net/plug-ins/1.13.7/i18n/zh-HANT.json'
        }
    });
}

function apiDataUrl() {
    return window.PROCURE_API_DATA || '/procure/api/data';
}

function apiUpdateUrl() {
    return window.PROCURE_API_UPDATE || '/procure/api/update';
}

async function fetchData() {
    const res = await fetch(apiDataUrl());
    if (!res.ok) {
        showAlert('載入資料失敗', 'danger');
        return;
    }
    const data = await res.json();
    renderTable(data);
}

async function sendUpdate(poNumber, field, value) {
    // deprecated path kept for backward compatibility
    const res = await fetch(apiUpdateUrl(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ po_number: poNumber, field, value })
    });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(text || '更新失敗');
    }
}

function bindEvents() {
    // 標記未儲存狀態（逐筆獨立）
    $('#procureTable').on('change input', '.arrival-input, .dispatch-input, .status-select, .remarks-input', function () {
        const rowIdRaw = $(this).data('id');
        const rowId = Number(rowIdRaw);
        if (Number.isNaN(rowId)) return;

        const tr = $(this).closest('tr');
        const masterTr = tr.hasClass('child') ? tr.prev() : tr;
        const row = dataTable.row(masterTr);
        const rowData = row.data();
        if (!rowData) return;

        const arrivalVal = $(`.arrival-input[data-id="${rowId}"]`).first().val() || '';
        const dispatchVal = $(`.dispatch-input[data-id="${rowId}"]`).first().val() || '';
        const statusVal = $(`.status-select[data-id="${rowId}"]`).first().val() || '';
        const remarksVal = $(`.remarks-input[data-id="${rowId}"]`).first().val() || '';

        masterTr.addClass('table-warning');
        dirtyMap.set(rowId, {
            arrival_date: arrivalVal,
            dispatch_date: dispatchVal,
            goods_status: statusVal,
            remarks: remarksVal,
            rowData
        });
    });

    // 全域儲存
    $('#save-btn').on('click', async function () {
        if (!CAN_EDIT) return;
        if (dirtyMap.size === 0) {
            showAlert('沒有變更需要儲存', 'danger');
            return;
        }
        try {
            for (const [rowId, payload] of dirtyMap.entries()) {
                const { arrival_date, dispatch_date, goods_status, remarks, rowData } = payload;
                const updates = [];
                if (formatDateValue(rowData.arrival_date) !== arrival_date) {
                    updates.push({ field: 'arrival_date', value: arrival_date });
                }
                if (formatDateValue(rowData.dispatch_date) !== dispatch_date) {
                    updates.push({ field: 'dispatch_date', value: dispatch_date });
                }
                if ((rowData.goods_status || '') !== goods_status) {
                    updates.push({ field: 'goods_status', value: goods_status });
                }
                if ((rowData.remarks || '') !== remarks) {
                    updates.push({ field: 'remarks', value: remarks });
                }

                for (const u of updates) {
                    const res = await fetch(apiUpdateUrl(), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            id: rowId,
                            po_number: rowData.po_number,
                            field: u.field,
                            value: u.value
                        })
                    });
                    if (!res.ok) {
                        const txt = await res.text();
                        throw new Error(txt || '更新失敗');
                    }
                    rowData[u.field] = u.value;
                }
                // 更新資料列狀態
                dataTable.rows(function (idx, data) { return Number(data.id) === Number(rowId); })
                    .every(function () {
                        this.data(Object.assign({}, rowData));
                        $(this.node()).removeClass('table-warning');
                    });
            }
            dirtyMap.clear();
            dataTable.draw(false);
            showAlert('已儲存變更');
        } catch (err) {
            showAlert(err.message, 'danger');
        }
    });

    $('#reload-btn').on('click', () => fetchData());

    // 匯出 Excel
    $('#export-btn').on('click', function () {
        if (!dataTable) {
            showAlert('表格資料尚未載入', 'danger');
            return;
        }

        const type = $('#typeFilter').val();
        const category = $('#categoryFilter').val();
        const status = $('#statusFilter').val();
        const search = dataTable.search();

        const params = new URLSearchParams({
            type: type,
            category: category,
            status: status,
            search: search,
        });

        const exportUrl = `/procure/export?${params.toString()}`;
        window.location.href = exportUrl;
    });

    // 庫存明細
    $('#procureTable').on('click', '.stock-detail', async function () {
        const code = $(this).data('code') || '';
        const name = $(this).data('name') || '';
        try {
            const qs = new URLSearchParams({ code, name });
            const res = await fetch(`/procure/api/stock_detail?${qs.toString()}`);
            if (!res.ok) throw new Error('庫存明細查詢失敗');
            const data = await res.json();
            const body = $('#stockModalBody');
            body.empty();
            (data.warehouses || []).forEach(item => {
                body.append(`<tr><td>${escapeHtml(item.name || '')}</td><td class="text-end">${item.qty ?? 0}</td></tr>`);
            });
            $('#stockModalTotal').text(data.total ?? 0);
            $('#stockModalProduct').text(name || code || '庫存');
            const modal = new bootstrap.Modal(document.getElementById('stockModal'));
            modal.show();
        } catch (err) {
            showAlert(err.message || '庫存明細查詢失敗', 'danger');
        }
    });

    // 過濾：來源類型 + 產品類別
    $.fn.dataTable.ext.search.push(function (settings, data, dataIndex) {
        const po = (data[0] || '').toString();
        const typeFilter = $('#typeFilter').val();
        if (typeFilter === 'OO' && !po.startsWith('OO')) return false;
        if (typeFilter === 'PO' && !po.startsWith('PO')) return false;

        const categoryFilter = $('#categoryFilter').val();
        if (categoryFilter && categoryFilter !== 'all') {
            const rowData = dataTable.row(dataIndex).data();
            const cat = getCategoryFromCode(rowData?.product_code || '');
            if (categoryFilter === 'other') {
                if (['10', '20', '21', '30'].includes(cat)) return false;
            } else if (cat !== categoryFilter) {
                return false;
            }
        }

        const statusFilter = $('#statusFilter').val();
        const rowData = dataTable.row(dataIndex).data();
        const statusVal = (rowData?.status || '').trim();
        if (statusFilter === 'active' && statusVal === '結案') return false;
        if (statusFilter === 'closed' && statusVal !== '結案') return false;

        // 隱藏未交貨數量 <= 0 (已完成採購)
        const q = Number(rowData?.quantity || 0);
        const w = Number(rowData?.warehouse_qty || 0);
        if (q - w <= 0) return false;
        return true;
    });

    $('#typeFilter, #categoryFilter, #statusFilter').on('change', function () {
        if (dataTable) dataTable.draw();
    });
}

$(document).ready(() => {
    bindEvents();
    fetchData();
});
