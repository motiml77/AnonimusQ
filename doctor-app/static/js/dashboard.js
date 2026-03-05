/* AnonimousQ – Dashboard calendar */

// Fallback defaults in case the Flask template didn't inject these
// (e.g. browser served a cached HTML that predates these variables)
if (typeof DEFAULT_DURATION === 'undefined') { var DEFAULT_DURATION = 45; }
if (typeof AVAILABILITY === 'undefined') {
  var AVAILABILITY = {
    workingDays: [1, 2, 3, 4, 5],
    workingHours: { start: '09:00', end: '17:00' },
    slotDurationMin: 45,
    blockedDates: [],
  };
}
if (typeof IS_OFFLINE === 'undefined') { var IS_OFFLINE = false; }
if (typeof ANON_IDS === 'undefined') { var ANON_IDS = []; }

// ─── Appointments cache (client-side, 60 s TTL) ────────────────────────────
// Prevents hammering /api/appointments on every FullCalendar view navigation.
// Any mutation (approve, reject, create, reschedule, mark) calls
// invalidateApptCache() so the next fetch is always fresh.
const _APPT_CACHE_TTL_MS = 60_000;
let _apptCacheEvents = null;
let _apptCacheFetchedAt = 0;

function invalidateApptCache() {
  _apptCacheEvents = null;
  _apptCacheFetchedAt = 0;
}

/** Wrapper: invalidate cache, re-fetch calendar, and trigger a background sync. */
function _refetchEvents() {
  invalidateApptCache();
  if (calendar) calendar.refetchEvents();
  _triggerSync();
}

// ─── Background Firebase sync ────────────────────────────────────────────
let _bgSyncInProgress = false;
let _bgSyncTimer = null;
const _BG_SYNC_INTERVAL_MS = 30_000; // 30 seconds

function _backgroundSync() {
  if (_bgSyncInProgress || IS_OFFLINE) return;
  _bgSyncInProgress = true;
  fetch('/api/appointments?sync=1')
    .then(r => r.json())
    .then(data => {
      _bgSyncInProgress = false;
      if (data.ok && data.events) {
        const oldJson = JSON.stringify(_apptCacheEvents || []);
        const newJson = JSON.stringify(data.events);
        if (oldJson !== newJson) {
          _apptCacheEvents = data.events;
          _apptCacheFetchedAt = Date.now();
          if (calendar) calendar.refetchEvents();
        }
      }
    })
    .catch(function() {
      _bgSyncInProgress = false;
    });
}

/** Schedule periodic sync every 30 seconds. */
function _startPeriodicSync() {
  if (_bgSyncTimer) return;
  _bgSyncTimer = setInterval(_backgroundSync, _BG_SYNC_INTERVAL_MS);
}

/** Trigger an immediate sync (after local mutation). */
function _triggerSync() {
  // Reset the periodic timer so we don't double-sync
  if (_bgSyncTimer) { clearInterval(_bgSyncTimer); _bgSyncTimer = null; }
  _backgroundSync();
  _startPeriodicSync();
}


/** Check if the app is in a state where the action can proceed.
 *  For operations with offline fallbacks this always returns true. */
function _checkOnline(action) {
  // All create/reschedule operations have local fallbacks, always allow
  return true;
}

/**
 * Only call this for operations that TRULY need a live Firebase connection
 * (currently just toggleOnlineBooking). All other operations have offline
 * fallbacks in the backend (local_appointments table).
 */
function _requiresFirebase(action) {
  if (IS_OFFLINE) {
    alert('פעולת "' + action + '" דורשת חיבור ל-Firebase.\nחבר Firebase בהגדרות ונסה שוב.');
    return false;
  }
  return true;
}

/**
 * Show/hide a loading spinner inside a button and toggle its disabled state.
 * @param {HTMLElement} btn  - the button element
 * @param {boolean}     busy - true = spinner on, false = restore
 */
function _setLoading(btn, busy) {
  if (!btn) return;
  if (busy) {
    btn.disabled = true;
    btn.dataset.originalHtml = btn.innerHTML;
    btn.innerHTML =
      '<span class="spinner-border spinner-border-sm" role="status" ' +
      'aria-hidden="true" style="width:.85rem;height:.85rem;border-width:2px;"></span>';
  } else {
    btn.disabled = false;
    if (btn.dataset.originalHtml) btn.innerHTML = btn.dataset.originalHtml;
  }
}

let calendar;

const PAYMENT_LABELS = {
  bit: 'ביט',
  paybox: 'פייבוקס',
  cash: 'מזומן',
  bank: 'העברה בנקאית',
};

// ─── Current appointment (for reschedule) ──────────────────────────────────
let _currentApptId = null;
let _currentApptSource = null;
let _currentApptDuration = 45;   // durationMin of the appointment being viewed

// ─── Manual-create duration ─────────────────────────────────────────────────
let _createDuration = 45;        // updated by the +/− stepper

function changeDuration(delta) {
  const def = (typeof DEFAULT_DURATION !== 'undefined') ? DEFAULT_DURATION : 45;
  _createDuration = Math.max(5, Math.min(240, _createDuration + delta));
  const el = document.getElementById('durationDisplay');
  if (el) el.textContent = _createDuration;
}

// ─── Context menu ──────────────────────────────────────────────────────────

let ctxMenu = null;

function _ensureCtxMenu() {
  if (ctxMenu) return ctxMenu;
  ctxMenu = document.createElement('div');
  ctxMenu.id = 'cal-ctx-menu';
  Object.assign(ctxMenu.style, {
    position: 'fixed', zIndex: '9999',
    background: '#fff',
    border: '1px solid #dee2e6', borderRadius: '8px',
    boxShadow: '0 4px 20px rgba(0,0,0,.15)',
    padding: '4px 0', minWidth: '200px', display: 'none',
  });
  document.body.appendChild(ctxMenu);
  document.addEventListener('click', () => { ctxMenu.style.display = 'none'; });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') ctxMenu.style.display = 'none'; });
  // Dismiss on any scroll (including FullCalendar's internal scroller)
  document.addEventListener('scroll', () => { ctxMenu.style.display = 'none'; }, true);
  return ctxMenu;
}

function _ctxItem(icon, label, color, onclick) {
  const btn = document.createElement('button');
  Object.assign(btn.style, {
    display: 'flex', alignItems: 'center', gap: '8px',
    width: '100%', padding: '8px 16px',
    border: 'none', background: 'none', cursor: 'pointer',
    fontSize: '.9rem', textAlign: 'right',
  });
  btn.innerHTML = `<span style="font-size:1rem;color:${color}">${icon}</span>${label}`;
  btn.onmouseenter = () => btn.style.background = '#f8f9fa';
  btn.onmouseleave = () => btn.style.background = 'none';
  btn.addEventListener('click', e => { e.stopPropagation(); onclick(); });
  return btn;
}

function _showContextMenu(x, y, eventId, source, status, treated, paid, paymentMethod) {
  const menu = _ensureCtxMenu();
  menu.innerHTML = '';

  if (status === 'pending') {
    // ── Pending: approve / reject only ──
    menu.appendChild(_ctxItem('✔', 'אשר תור', '#198754',
      () => { menu.style.display = 'none'; approveAppt(eventId, source); }
    ));
    menu.appendChild(_ctxItem('✕', 'דחה תור', '#dc3545',
      () => { menu.style.display = 'none'; rejectAppt(eventId, source); }
    ));
  } else if (status === 'cancel_requested') {
    // ── Cancel requested: approve cancel / reject cancel ──
    menu.appendChild(_ctxItem('✔', 'אשר ביטול', '#dc3545',
      () => { menu.style.display = 'none'; approveCancelReq(eventId, source); }
    ));
    menu.appendChild(_ctxItem('✕', 'דחה בקשת ביטול', '#0d6efd',
      () => { menu.style.display = 'none'; rejectCancelReq(eventId, source); }
    ));
  } else {
    // ── Booked: treat + payment ──
    menu.appendChild(_ctxItem(
      treated ? '✔' : '○', 'טיפול בוצע', '#0d6efd',
      () => { menu.style.display = 'none'; _markAppt(eventId, source, 'treated', !treated, null); }
    ));

    // Payment item (with sub-panel) – disabled until treated
    const payWrap = document.createElement('div');
    payWrap.style.position = 'relative';

    const methodLabel = paymentMethod ? PAYMENT_LABELS[paymentMethod] : '';
    const payBtn = document.createElement('button');
    const payDisabled = !treated && !paid;
    Object.assign(payBtn.style, {
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      width: '100%', padding: '8px 16px',
      border: 'none', background: 'none',
      cursor: payDisabled ? 'not-allowed' : 'pointer',
      fontSize: '.9rem',
      opacity: payDisabled ? '.45' : '1',
    });
    payBtn.innerHTML =
      `<span style="display:flex;align-items:center;gap:8px;">` +
      `<span style="font-size:1rem;color:#198754">${paid ? '✔' : '○'}</span>` +
      `תשלום${methodLabel ? ' (' + methodLabel + ')' : ''}` +
      `</span>` + (payDisabled
        ? `<span style="font-size:.7rem;color:#adb5bd;">סמן תחילה שטיפול בוצע</span>`
        : `<span style="font-size:.75rem;color:#adb5bd;">▶</span>`);
    if (!payDisabled) {
      payBtn.onmouseenter = () => { payBtn.style.background = '#f8f9fa'; sub.style.display = 'block'; };
      payBtn.onmouseleave = () => { payBtn.style.background = 'none'; };
    }

    const sub = document.createElement('div');
    Object.assign(sub.style, {
      position: 'absolute', top: '0', right: '100%',
      background: '#fff', border: '1px solid #dee2e6', borderRadius: '8px',
      boxShadow: '0 4px 16px rgba(0,0,0,.13)',
      padding: '4px 0', minWidth: '170px', display: 'none', zIndex: '10000',
    });
    sub.addEventListener('mouseenter', () => { sub.style.display = 'block'; });
    sub.addEventListener('mouseleave', () => { sub.style.display = 'none'; });

    Object.entries(PAYMENT_LABELS).forEach(([key, label]) => {
      const mi = _ctxItem(
        paymentMethod === key ? '✔' : '　',
        label, '#198754',
        () => { menu.style.display = 'none'; _markAppt(eventId, source, 'paid', true, key); }
      );
      sub.appendChild(mi);
    });

    if (paid) {
      const sep2 = document.createElement('hr');
      sep2.style.cssText = 'margin:4px 8px;border-color:#e9ecef;';
      sub.appendChild(sep2);
      sub.appendChild(_ctxItem('✕', 'בטל תשלום', '#dc3545',
        () => { menu.style.display = 'none'; _markAppt(eventId, source, 'paid', false, null); }
      ));
    }

    payWrap.appendChild(payBtn);
    payWrap.appendChild(sub);
    payWrap.addEventListener('mouseleave', () => { sub.style.display = 'none'; });
    menu.appendChild(payWrap);
  }

  // ── Separator + Delete (always) ──
  const sep = document.createElement('hr');
  sep.style.cssText = 'margin:4px 8px;border-color:#e9ecef;';
  menu.appendChild(sep);
  menu.appendChild(_ctxItem('🗑', 'מחק תור', '#dc3545', () => {
    menu.style.display = 'none';
    _deleteAppt(eventId, source);
  }));

  menu.style.left = x + 'px';
  menu.style.top = y + 'px';
  menu.style.display = 'block';

  // Keep inside viewport
  requestAnimationFrame(() => {
    const r = menu.getBoundingClientRect();
    if (r.right > window.innerWidth) menu.style.left = (x - r.width) + 'px';
    if (r.bottom > window.innerHeight) menu.style.top = (y - r.height) + 'px';
  });
}

function _markAppt(id, source, field, value, paymentMethod) {
  // No online check needed – the backend stores locally when offline

  // Optimistic UI: update event badges immediately
  const evEl = document.querySelector('[data-event-id="' + id + '"]');
  const ev = calendar.getEventById(id);
  if (ev) {
    ev.setExtendedProp(field, value);
    if (field === 'paid' && value) ev.setExtendedProp('paymentMethod', paymentMethod);
    if (field === 'paid' && !value) ev.setExtendedProp('paymentMethod', null);
    if (field === 'treated' && !value) {
      ev.setExtendedProp('paid', false);
      ev.setExtendedProp('paymentMethod', null);
    }
    if (evEl) _rerenderBadges(evEl, ev.extendedProps);
  }

  fetch('/appointments/mark/' + id + '?source=' + (source || 'firebase'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ field, value, paymentMethod }),
  })
    .then(r => r.json())
    .then(data => {
      if (!data.ok) alert('שגיאה: ' + (data.error || 'לא ידוע'));
      _refetchEvents();
    })
    .catch(err => { alert('שגיאת רשת: ' + err); _refetchEvents(); });
}

function _rerenderBadges(el, props) {

  var old = el.querySelector('[data-badges]');
  if (old) old.remove();
  var badges = document.createElement('span');
  badges.setAttribute('data-badges', '1');
  badges.style.cssText = 'position:absolute;top:2px;left:4px;display:flex;gap:2px;pointer-events:none;';
  if (props.treated) {
    var b = document.createElement('span');
    b.title = 'טיפול בוצע';
    b.style.cssText = 'color:#fff;background:#0d6efd;border-radius:3px;padding:0 3px;font-size:.68rem;font-weight:700;';
    b.textContent = '✔';
    badges.appendChild(b);
  }
  if (props.paid) {
    var b2 = document.createElement('span');
    var pm = PAYMENT_LABELS[props.paymentMethod];
    b2.title = pm ? 'שולם – ' + pm : 'תשלום התקבל';
    b2.style.cssText = 'color:#fff;background:#198754;border-radius:3px;padding:0 3px;font-size:.68rem;font-weight:700;';
    b2.textContent = pm ? pm[0] + '₪' : '₪';
    badges.appendChild(b2);
  }
  if (props.patientMarkedPaid && !props.paid) {
    var b3 = document.createElement('span');
    var ppm = PAYMENT_LABELS[props.patientPaymentMethod];
    b3.title = ppm ? 'המטופל דיווח: שולם ב' + ppm + ' – ממתין לאימות' : 'המטופל דיווח על תשלום – ממתין לאימות';
    b3.style.cssText = 'color:#fff;background:#6f42c1;border-radius:3px;padding:0 3px;font-size:.68rem;font-weight:700;';
    b3.textContent = ppm ? ppm[0] + '₪?' : '₪?';
    badges.appendChild(b3);
  }
  if (badges.children.length) {
    el.style.position = 'relative';
    el.appendChild(badges);
  }
}

// ─── Calendar ──────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', function () {
  const calEl = document.getElementById('calendar');
  if (!calEl) return;

  // Calculate dynamic time range: 1 hour buffer around working hours
  const _workStart = (AVAILABILITY.workingHours || {}).start || '09:00';
  const _workEnd = (AVAILABILITY.workingHours || {}).end || '17:00';
  const _minHour = Math.max(0, parseInt(_workStart) - 1);
  const _maxHour = Math.min(24, parseInt(_workEnd) + 1);

  calendar = new FullCalendar.Calendar(calEl, {
    initialView: 'timeGridWeek',
    locale: 'he',
    direction: 'rtl',
    headerToolbar: {
      start: 'prev,next today',
      center: 'title',
      end: 'dayGridMonth,timeGridWeek,timeGridDay',
    },
    buttonText: { today: 'היום', month: 'חודש', week: 'שבוע', day: 'יום' },
    allDaySlot: false,                               // doctors don't need all-day row
    navLinks: true,                                 // click day name → day view
    slotMinTime: String(_minHour).padStart(2, '0') + ':00:00',
    slotMaxTime: String(_maxHour).padStart(2, '0') + ':00:00',
    slotDuration: '00:30:00',
    snapDuration: '00:15:00',                     // click precision: 15 min
    slotLabelInterval: '01:00:00',
    slotLabelFormat: { hour: '2-digit', minute: '2-digit', hour12: false },
    scrollTime: _workStart + ':00',
    businessHours: {
      daysOfWeek: AVAILABILITY.workingDays || [0, 1, 2, 3, 4],
      startTime: _workStart,
      endTime: _workEnd,
    },
    height: 'calc(100vh - 140px)',
    expandRows: true,
    nowIndicator: true,
    eventColor: '#0d6efd',
    eventTimeFormat: { hour: '2-digit', minute: '2-digit', meridiem: false, hour12: false },
    dayMaxEvents: 3,                                   // month view: collapse after 3

    events: function (info, successCb, failureCb) {
      const now = Date.now();
      if (_apptCacheEvents && (now - _apptCacheFetchedAt) < _APPT_CACHE_TTL_MS) {
        successCb(_apptCacheEvents);
        return;
      }
      // Phase 1: fast local load (cached + local appointments)
      fetch('/api/appointments')
        .then(r => r.json())
        .then(data => {
          if (data.ok) {
            _apptCacheEvents = data.events;
            _apptCacheFetchedAt = Date.now();
            successCb(data.events);
            // Start periodic background sync (every 30s)
            _startPeriodicSync();
          } else {
            failureCb(data.error);
          }
        })
        .catch(failureCb);
    },

    // After all events load/change – detect overlapping events, update pending banner
    eventsSet: function (events) {
      const all = events.filter(e => e.start && e.end).map(e => ({
        id: e.id,
        start: e.start.getTime(),
        end: e.end.getTime(),
      }));
      const conflictIds = new Set();
      for (let i = 0; i < all.length; i++) {
        for (let j = i + 1; j < all.length; j++) {
          if (all[i].start < all[j].end && all[i].end > all[j].start) {
            conflictIds.add(all[i].id);
            conflictIds.add(all[j].id);
          }
        }
      }
      document.querySelectorAll('.fc-event[data-event-id]').forEach(el => {
        el.classList.toggle('event-conflict', conflictIds.has(el.dataset.eventId));
      });

      // Update pending appointments banner + cancel requests banner
      _updatePendingBanner(events);
      _updateCancelBanner(events);
    },

    // Render badges + pending animation + right-click
    eventDidMount: function (info) {
      const props = info.event.extendedProps;

      // Tag element for eventsSet overlap detection
      info.el.setAttribute('data-event-id', info.event.id);

      if (props.status === 'pending') info.el.classList.add('event-pending');

      // Overlay badges (treated ✔ and paid ₪/method)
      const badges = document.createElement('span');
      badges.setAttribute('data-badges', '1');
      badges.style.cssText = 'position:absolute;top:2px;left:4px;display:flex;gap:2px;pointer-events:none;';
      if (props.treated) {
        const b = document.createElement('span');
        b.title = 'טיפול בוצע';
        b.style.cssText = 'color:#fff;background:#0d6efd;border-radius:3px;padding:0 3px;font-size:.68rem;font-weight:700;';
        b.textContent = '✔';
        badges.appendChild(b);
      }
      if (props.paid) {
        const b = document.createElement('span');
        const pm = PAYMENT_LABELS[props.paymentMethod];
        b.title = pm ? 'שולם – ' + pm : 'תשלום התקבל';
        b.style.cssText = 'color:#fff;background:#198754;border-radius:3px;padding:0 3px;font-size:.68rem;font-weight:700;';
        b.textContent = pm ? pm[0] + '₪' : '₪';
        badges.appendChild(b);
      }
      if (props.patientMarkedPaid && !props.paid) {
        const b = document.createElement('span');
        const ppm = PAYMENT_LABELS[props.patientPaymentMethod];
        b.title = ppm ? 'המטופל דיווח: שולם ב' + ppm + ' – ממתין לאימות' : 'המטופל דיווח על תשלום – ממתין לאימות';
        b.style.cssText = 'color:#fff;background:#6f42c1;border-radius:3px;padding:0 3px;font-size:.68rem;font-weight:700;';
        b.textContent = ppm ? ppm[0] + '₪?' : '₪?';
        badges.appendChild(b);
      }
      if (badges.children.length) {
        info.el.style.position = 'relative';
        info.el.appendChild(badges);
      }

      // Anonymous patient blur on event title (toggle reveal without blocking eventClick)
      if (ANON_IDS.indexOf(props.anonymousId) !== -1) {
        const titleEl = info.el.querySelector('.fc-event-title');
        if (titleEl) {
          titleEl.classList.add('anon-blur');
          titleEl.addEventListener('dblclick', function (e) {
            e.stopPropagation();
            this.classList.toggle('revealed');
          });
        }
      }

      // Right-click → context menu
      info.el.addEventListener('contextmenu', e => {
        e.preventDefault();
        _showContextMenu(e.clientX, e.clientY,
          info.event.id, props.source, props.status, props.treated, props.paid, props.paymentMethod);
      });
    },

    // Left-click → details + approve/reject modal
    eventClick: function (info) {
      const ev = info.event;
      const props = ev.extendedProps;

      // Store for reschedule
      _currentApptId = ev.id;
      _currentApptSource = props.source;
      _currentApptDuration = props.durationMin || (typeof DEFAULT_DURATION !== 'undefined' ? DEFAULT_DURATION : 45);

      const timeStr = ev.start ? ev.start.toLocaleTimeString('he-IL', {
        hour: '2-digit', minute: '2-digit', hour12: false,
      }) : '';
      const dateStr = ev.start ? ev.start.toLocaleDateString('he-IL', {
        weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
      }) : '';

      // Anonymous blur in modal
      const nameEl = document.getElementById('apptModalName');
      nameEl.textContent = ev.title;
      nameEl.classList.remove('anon-blur', 'revealed');
      if (ANON_IDS.indexOf(props.anonymousId) !== -1) {
        nameEl.classList.add('anon-blur');
        nameEl.style.cursor = 'pointer';
        nameEl.onclick = function () { this.classList.toggle('revealed'); };
      } else {
        nameEl.style.cursor = '';
        nameEl.onclick = null;
      }
      document.getElementById('apptModalDate').textContent = dateStr;
      document.getElementById('apptModalTime').textContent = timeStr;

      // Pre-fill reschedule form with current values
      if (ev.start) {
        document.getElementById('rescheduleDate').value = ev.start.toISOString().substring(0, 10);
        document.getElementById('rescheduleTime').value = ev.start.toISOString().substring(11, 16);
      }
      document.getElementById('rescheduleSection').style.display = 'none';
      document.getElementById('rescheduleError').classList.add('d-none');

      const badge = document.getElementById('apptModalStatus');
      if (props.status === 'cancel_requested') {
        badge.textContent = 'בקשת ביטול'; badge.className = 'badge bg-danger fs-6';
      } else if (props.status === 'pending') {
        badge.textContent = 'ממתין לאישור'; badge.className = 'badge bg-warning text-dark fs-6';
      } else {
        badge.textContent = 'מאושר'; badge.className = 'badge bg-success fs-6';
      }

      // ── Check for overlapping appointments ──
      const conflictWarn = document.getElementById('apptConflictWarning');
      const conflictText = document.getElementById('apptConflictText');
      conflictWarn.classList.add('d-none');
      if (ev.start && ev.end) {
        const evStart = ev.start.getTime();
        const evEnd = ev.end.getTime();
        const overlaps = [];
        calendar.getEvents().forEach(other => {
          if (other.id === ev.id) return;
          if (!other.start || !other.end) return;
          const oStart = other.start.getTime();
          const oEnd = other.end.getTime();
          if (evStart < oEnd && evEnd > oStart) {
            overlaps.push(other);
          }
        });
        if (overlaps.length > 0) {
          const names = overlaps.map(o => {
            const t = o.start.toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', hour12: false });
            const st = (o.extendedProps.status === 'pending') ? 'ממתין' : 'מאושר';
            return `${o.title} (${t}, ${st})`;
          }).join(', ');
          conflictText.textContent = `יש חפיפה עם: ${names}`;
          conflictWarn.classList.remove('d-none');
        }
      }

      const approveBtn = document.getElementById('apptApproveBtn');
      const rejectBtn = document.getElementById('apptRejectBtn');
      const approveCancelBtn = document.getElementById('apptApproveCancelBtn');
      const rejectCancelBtn = document.getElementById('apptRejectCancelBtn');

      approveBtn.style.display = rejectBtn.style.display = 'none';
      approveCancelBtn.style.display = rejectCancelBtn.style.display = 'none';

      if (props.status === 'pending') {
        approveBtn.style.display = rejectBtn.style.display = '';
        approveBtn.onclick = () => approveAppt(ev.id, props.source);
        rejectBtn.onclick = () => rejectAppt(ev.id, props.source);
      } else if (props.status === 'cancel_requested') {
        approveCancelBtn.style.display = rejectCancelBtn.style.display = '';
        approveCancelBtn.onclick = function () {
          bootstrap.Modal.getInstance(document.getElementById('apptModal'))?.hide();
          approveCancelReq(ev.id, props.source);
        };
        rejectCancelBtn.onclick = function () {
          bootstrap.Modal.getInstance(document.getElementById('apptModal'))?.hide();
          rejectCancelReq(ev.id, props.source);
        };
      }

      // ── Treated / Paid actions in modal (only for booked appointments) ──
      const actionsSection = document.getElementById('apptActionsSection');
      if (props.status === 'pending' || props.status === 'cancel_requested') {
        actionsSection.style.display = 'none';
      } else {
        actionsSection.style.display = '';
        _setupModalActions(ev.id, props.source, props.treated, props.paid,
                           props.paymentMethod, props.patientMarkedPaid, props.patientPaymentMethod);
      }

      new bootstrap.Modal(document.getElementById('apptModal')).show();
    },

    // Custom column header: week/day view shows day name + date, month view shows day name only
    dayHeaderContent: function (info) {
      const DAY_NAMES = ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת'];
      const dayName = DAY_NAMES[info.date.getDay()];
      const viewType = info.view.type;

      const wrap = document.createElement('div');
      wrap.style.cssText = 'display:flex;flex-direction:column;align-items:center;line-height:1.3;';

      const nameEl = document.createElement('span');
      nameEl.style.cssText = 'font-size:.7rem;font-weight:700;letter-spacing:.04em;color:#212529;';
      nameEl.textContent = dayName;
      wrap.appendChild(nameEl);

      // In week/day view: show the date number below the day name
      if (viewType === 'timeGridWeek' || viewType === 'timeGridDay') {
        const dateEl = document.createElement('span');
        dateEl.style.cssText = 'font-size:.82rem;font-weight:600;color:#495057;';
        dateEl.textContent = info.date.getDate() + '/' + (info.date.getMonth() + 1);
        wrap.appendChild(dateEl);
      }

      return { domNodes: [wrap] };
    },

    loading: function (isLoading) {
      if (!isLoading) {
        // First load complete – hide loading overlay, reveal calendar + legend
        const overlay = document.getElementById('dashboard-loading');
        const calDiv = document.getElementById('calendar');
        const legend = document.getElementById('calendarLegend');
        if (overlay) overlay.style.display = 'none';
        if (calDiv) calDiv.style.display = '';
        if (legend) legend.style.display = '';
      }
    },

    // Click on empty slot → open manual booking modal
    dateClick: function (info) {
      if (!_checkOnline('לקבוע תור')) return;
      const dateStr = info.dateStr.substring(0, 10);
      const timeStr = info.dateStr.length > 10 ? info.dateStr.substring(11, 16) : '';
      document.getElementById('createDateInput').value = dateStr;
      document.getElementById('createTimeInput').value = timeStr;
      document.getElementById('createPatientInput').value = '';
      document.getElementById('createApptError').classList.add('d-none');
      // Reset walk-in toggle
      const walkInToggle = document.getElementById('walkInToggle');
      if (walkInToggle) { walkInToggle.checked = false; toggleWalkIn(); }
      // Reset duration to global default
      _createDuration = (typeof DEFAULT_DURATION !== 'undefined') ? DEFAULT_DURATION : 45;
      const durEl = document.getElementById('durationDisplay');
      if (durEl) durEl.textContent = _createDuration;
      new bootstrap.Modal(document.getElementById('createApptModal')).show();
    },
  });

  calendar.render();
});

// ─── Approve / Reject ─────────────────────────────────────────────────────

function approveAppt(id, source) {
  const btn = document.getElementById('apptApproveBtn');
  const modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
  _setLoading(btn, true);
  if (modal) modal.hide();
  fetch('/appointments/approve/' + id + '?source=' + (source || 'firebase'), { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      _setLoading(btn, false);
      if (!data.ok) alert('שגיאה: ' + (data.error || 'לא ידוע'));
      _refetchEvents();
    })
    .catch(err => { _setLoading(btn, false); alert('שגיאת רשת: ' + err); _refetchEvents(); });
}

function rejectAppt(id, source) {
  if (!confirm('האם לדחות את התור?')) return;
  const btn = document.getElementById('apptRejectBtn');
  const modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
  _setLoading(btn, true);
  if (modal) modal.hide();
  fetch('/appointments/reject/' + id + '?source=' + (source || 'firebase'), { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      _setLoading(btn, false);
      if (!data.ok) alert('שגיאה: ' + (data.error || 'לא ידוע'));
      _refetchEvents();
    })
    .catch(err => { _setLoading(btn, false); alert('שגיאת רשת: ' + err); _refetchEvents(); });
}

// ─── Manual create appointment ─────────────────────────────────────────────

const WALKIN_ID = 'WALKIN';

function toggleWalkIn() {
  const isWalkIn = document.getElementById('walkInToggle').checked;
  document.getElementById('patientInputGroup').classList.toggle('d-none', isWalkIn);
  document.getElementById('walkInNote').classList.toggle('d-none', !isWalkIn);
  if (isWalkIn) document.getElementById('createPatientInput').value = '';
}

function _extractPatientId(inputValue) {
  // Input format: "שם מטופל (A234B)" — extract the ID from the parens
  const match = inputValue.match(/\(([^)]+)\)\s*$/);
  return match ? match[1].trim() : inputValue.trim();
}

function submitCreateAppt() {
  if (!_checkOnline('לקבוע תור')) return;
  const isWalkIn = document.getElementById('walkInToggle').checked;
  const rawInput = document.getElementById('createPatientInput').value.trim();
  const date = document.getElementById('createDateInput').value;
  const time = document.getElementById('createTimeInput').value;
  const errEl = document.getElementById('createApptError');
  const anonymousId = isWalkIn ? WALKIN_ID : _extractPatientId(rawInput);

  if (!anonymousId || !date || !time) {
    errEl.textContent = isWalkIn ? 'יש לבחור תאריך ושעה' : 'יש למלא את כל השדות';
    errEl.classList.remove('d-none');
    return;
  }

  // Close modal immediately for instant feel
  bootstrap.Modal.getInstance(document.getElementById('createApptModal')).hide();

  fetch('/appointments/create', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ anonymousId, date, time, durationMin: _createDuration }),
  })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        _refetchEvents();
      } else {
        alert(data.error || 'שגיאה ביצירת תור');
        _refetchEvents();
      }
    })
    .catch(err => {
      alert('שגיאת רשת: ' + err);
    });
}

// ─── Reschedule appointment ─────────────────────────────────────────────────

function toggleReschedule() {
  const sec = document.getElementById('rescheduleSection');
  sec.style.display = sec.style.display === 'none' ? '' : 'none';
}

function submitReschedule() {
  if (!_checkOnline('לשנות תור')) return;
  const date = document.getElementById('rescheduleDate').value;
  const time = document.getElementById('rescheduleTime').value;
  const errEl = document.getElementById('rescheduleError');

  if (!date || !time) {
    errEl.textContent = 'יש לבחור תאריך ושעה';
    errEl.classList.remove('d-none');
    return;
  }

  // Close modal immediately for instant feel
  bootstrap.Modal.getInstance(document.getElementById('apptModal')).hide();

  fetch('/appointments/reschedule/' + _currentApptId + '?source=' + (_currentApptSource || 'firebase'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ date, time, durationMin: _currentApptDuration }),
  })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        _refetchEvents();
      } else {
        alert(data.error || 'שגיאה בשינוי תור');
        _refetchEvents();
      }
    })
    .catch(err => {
      alert('שגיאת רשת: ' + err);
    });
}

// ─── Toggle online booking ──────────────────────────────────────────────────

function toggleOnlineBooking() {
  if (!_requiresFirebase('שינוי הגדרת תורים')) return;

  const btn = document.getElementById('onlineBookingToggle');
  const isCurrentlyEnabled = btn.textContent.trim().includes('פעילים');

  if (isCurrentlyEnabled) {
    if (!confirm('האם לנטרל קביעת תורים אונליין?\n\nמטופלים לא יוכלו לקבוע תורים חדשים באתר.\nתורים קיימים ימשיכו להופיע.'))
      return;
  }

  // Optimistic UI: update button instantly
  if (isCurrentlyEnabled) {
    btn.innerHTML = '&#128683; תורים אונליין מנוטרלים';
    btn.style.background = '#dc3545';
    btn.style.borderColor = '#dc3545';
    btn.style.animation = 'btnDisabledPulse 1.5s ease-in-out infinite';
  } else {
    btn.innerHTML = '&#9989; תורים אונליין פעילים';
    btn.style.background = '#198754';
    btn.style.borderColor = '#198754';
    btn.style.animation = 'none';
  }

  fetch('/api/toggle-online-booking', { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (!data.ok) {
        // Revert on error
        alert('שגיאה: ' + (data.error || ''));
        if (isCurrentlyEnabled) {
          btn.innerHTML = '&#9989; תורים אונליין פעילים';
          btn.style.background = '#198754';
          btn.style.borderColor = '#198754';
          btn.style.animation = 'none';
        } else {
          btn.innerHTML = '&#128683; תורים אונליין מנוטרלים';
          btn.style.background = '#dc3545';
          btn.style.borderColor = '#dc3545';
          btn.style.animation = 'btnDisabledPulse 1.5s ease-in-out infinite';
        }
      }
    })
    .catch(err => {
      alert('שגיאת רשת: ' + err);
      // Revert on network error
      if (isCurrentlyEnabled) {
        btn.innerHTML = '&#9989; תורים אונליין פעילים';
        btn.style.background = '#198754';
        btn.style.borderColor = '#198754';
        btn.style.animation = 'none';
      } else {
        btn.innerHTML = '&#128683; תורים אונליין מנוטרלים';
        btn.style.background = '#dc3545';
        btn.style.borderColor = '#dc3545';
        btn.style.animation = 'btnDisabledPulse 1.5s ease-in-out infinite';
      }
    });
}

// ─── Delete appointment ───────────────────────────────────────────────────

function _deleteAppt(id, source) {
  if (!confirm('האם למחוק את התור לצמיתות?')) return;

  fetch('/appointments/delete/' + id + '?source=' + (source || 'firebase'), { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (!data.ok) alert('שגיאה: ' + (data.error || 'לא ידוע'));
      _refetchEvents();
    })
    .catch(err => { alert('שגיאת רשת: ' + err); _refetchEvents(); });
}

// ─── Modal treated/paid/delete actions ────────────────────────────────────

let _modalApptId = null;
let _modalApptSource = null;
let _modalTreated = false;
let _modalPaid = false;

function _setupModalActions(id, source, treated, paid, paymentMethod, patientMarkedPaid, patientPaymentMethod) {
  _modalApptId = id;
  _modalApptSource = source;
  _modalTreated = !!treated;
  _modalPaid = !!paid;

  // ── 1. Treated button ──
  const treatedBtn = document.getElementById('apptTreatedBtn');
  const treatedIcon = document.getElementById('apptTreatedIcon');
  if (treated) {
    treatedBtn.className = 'btn btn-sm btn-primary px-3';
    treatedIcon.innerHTML = '&#10004;';
  } else {
    treatedBtn.className = 'btn btn-sm btn-outline-secondary px-3';
    treatedIcon.innerHTML = '&#9675;';
  }

  // ── 2. Payment section ──
  const payDisabled = document.getElementById('apptPaymentDisabled');
  const payRow = document.getElementById('apptPaymentRow');
  const paidIcon = document.getElementById('apptPaidIcon');
  const cancelPayRow = document.getElementById('apptCancelPayRow');
  const cancelPayBtn = document.getElementById('apptCancelPayBtn');

  if (treated) {
    // Show payment buttons, hide disabled hint
    payDisabled.style.display = 'none';
    payRow.style.cssText = '';

    // Paid icon
    paidIcon.innerHTML = paid ? '&#10004;' : '&#9675;';

    // Highlight current payment method
    payRow.querySelectorAll('.modal-pay-btn').forEach(btn => {
      if (paid && btn.dataset.method === paymentMethod) {
        btn.className = 'btn btn-success btn-sm modal-pay-btn';
      } else {
        btn.className = 'btn btn-outline-success btn-sm modal-pay-btn';
      }
      btn.onclick = function () {
        var modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
        if (modal) modal.hide();
        _markAppt(id, source, 'paid', true, btn.dataset.method);
      };
    });

    // Cancel payment button
    if (paid) {
      cancelPayRow.classList.remove('d-none');
      cancelPayBtn.onclick = function () {
        var modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
        if (modal) modal.hide();
        _markAppt(id, source, 'paid', false, null);
      };
    } else {
      cancelPayRow.classList.add('d-none');
    }
  } else {
    // Not treated: show disabled hint, hide payment buttons
    payDisabled.style.display = '';
    payRow.style.cssText = 'display:none !important;';
  }

  // ── 3. Patient-reported payment info (purple section) ──
  const patPayInfo = document.getElementById('apptPatientPayInfo');
  const patPayTitle = document.getElementById('apptPatientPayTitle');
  const patPayText = document.getElementById('apptPatientPayText');
  if (patientMarkedPaid && !paid) {
    patPayInfo.classList.remove('d-none');
    const methodName = PAYMENT_LABELS[patientPaymentMethod] || '';
    patPayTitle.textContent = methodName
      ? 'המטופל דיווח: שולם ב' + methodName
      : 'המטופל דיווח על תשלום';
    patPayText.textContent = 'לחץ על אמצעי התשלום כדי לאשר קבלה:';

    // Highlight the patient's reported method and wire up confirm buttons
    patPayInfo.querySelectorAll('.modal-confirm-pay-btn').forEach(btn => {
      if (btn.dataset.method === patientPaymentMethod) {
        btn.style.background = '#5b21b6';
        btn.style.boxShadow = '0 0 0 2px #c084fc';
        btn.style.transform = 'scale(1.05)';
      } else {
        btn.style.background = '#7c3aed';
        btn.style.boxShadow = 'none';
        btn.style.transform = '';
      }
      btn.onclick = function () {
        var modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
        if (modal) modal.hide();
        _markAppt(id, source, 'paid', true, btn.dataset.method);
      };
    });
  } else {
    patPayInfo.classList.add('d-none');
  }
}

function toggleModalTreated() {
  if (!_modalApptId) return;
  const newVal = !_modalTreated;
  const modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
  if (modal) modal.hide();
  _markAppt(_modalApptId, _modalApptSource, 'treated', newVal, null);
}

function deleteFromModal() {
  if (!_modalApptId) return;
  const modal = bootstrap.Modal.getInstance(document.getElementById('apptModal'));
  if (modal) modal.hide();
  _deleteAppt(_modalApptId, _modalApptSource);
}

// ─── Pending appointments banner ──────────────────────────────────────────

let _pendingDropdownOpen = false;

function _updatePendingBanner(events) {
  const banner = document.getElementById('pendingBanner');
  if (!banner) return;

  const pending = events.filter(e => e.extendedProps && e.extendedProps.status === 'pending');
  if (pending.length === 0) {
    banner.style.display = 'none';
    return;
  }

  banner.style.display = '';
  document.getElementById('pendingBannerText').textContent =
    pending.length + ' תורים ממתינים לאישור';

  // Build dropdown items
  const dropdown = document.getElementById('pendingDropdown');
  const sorted = [...pending].sort((a, b) => {
    const aKey = (a.start || '').toString();
    const bKey = (b.start || '').toString();
    return aKey.localeCompare(bKey);
  });

  dropdown.innerHTML = sorted.map(ev => {
    const dateIso = ev.start ? ev.start.toISOString().split('T')[0] : '';
    // Short day name + d.m.yy format
    const dayName = ev.start ? ev.start.toLocaleDateString('he-IL', { weekday: 'short' }) : '';
    const d = ev.start ? ev.start.getDate() : '';
    const m = ev.start ? (ev.start.getMonth() + 1) : '';
    const y = ev.start ? String(ev.start.getFullYear()).slice(-2) : '';
    const dateShort = d + '.' + m + '.' + y;
    const timeStr = ev.start ? ev.start.toLocaleTimeString('he-IL', {
      hour: '2-digit', minute: '2-digit', hour12: false
    }) : '';
    return '<div class="pending-item" data-date="' + dateIso + '" data-id="' + ev.id + '" ' +
      'style="padding:6px 14px;border-bottom:1px solid #fde68a;cursor:pointer;display:flex;align-items:center;justify-content:space-between;font-size:.8rem;" ' +
      'onmouseenter="this.style.background=\'#fef3c7\'" onmouseleave="this.style.background=\'transparent\'">' +
      '<div style="display:flex;align-items:center;gap:6px;">' +
      '<span style="color:#e6a817;font-size:.7rem;">&#9679;</span>' +
      '<span style="font-weight:600;color:#856404;">' + ev.title + '</span>' +
      '</div>' +
      '<div style="display:flex;align-items:center;gap:6px;font-size:.78rem;color:#92400e;">' +
      '<span style="font-weight:600;">' + dayName + '</span>' +
      '<span>' + dateShort + '</span>' +
      '<span style="font-weight:700;">' + timeStr + '</span>' +
      '</div>' +
      '</div>';
  }).join('');

  // Wire up click handlers to navigate calendar to that date
  dropdown.querySelectorAll('.pending-item').forEach(item => {
    item.addEventListener('click', function () {
      const dateStr = this.dataset.date;
      const evId = this.dataset.id;
      if (dateStr && calendar) {
        calendar.gotoDate(dateStr);
        calendar.changeView('timeGridDay', dateStr);
        // Close dropdown
        _pendingDropdownOpen = false;
        dropdown.style.display = 'none';
        document.getElementById('pendingBannerArrow').style.transform = '';
        // Highlight the event after a short delay (let calendar render)
        setTimeout(function () {
          var el = document.querySelector('[data-event-id="' + evId + '"]');
          if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
            el.style.transition = 'box-shadow .3s';
            el.style.boxShadow = '0 0 0 3px #ffc107, 0 0 12px rgba(255,193,7,.5)';
            setTimeout(function () { el.style.boxShadow = ''; }, 2000);
          }
        }, 300);
      }
    });
  });
}

function togglePendingDropdown() {
  const dropdown = document.getElementById('pendingDropdown');
  const arrow = document.getElementById('pendingBannerArrow');
  _pendingDropdownOpen = !_pendingDropdownOpen;
  dropdown.style.display = _pendingDropdownOpen ? '' : 'none';
  arrow.style.transform = _pendingDropdownOpen ? 'rotate(180deg)' : '';
}

// ─── Cancel requests banner (red) ──────────────────────────────────────────

let _cancelDropdownOpen = false;

function _updateCancelBanner(events) {
  const banner = document.getElementById('cancelBanner');
  if (!banner) return;

  const cancelReqs = events.filter(e => e.extendedProps && e.extendedProps.status === 'cancel_requested');
  if (cancelReqs.length === 0) {
    banner.style.display = 'none';
    return;
  }

  banner.style.display = '';
  document.getElementById('cancelBannerText').textContent =
    cancelReqs.length + ' בקשות ביטול';

  const dropdown = document.getElementById('cancelDropdown');
  const sorted = [...cancelReqs].sort((a, b) => {
    const aKey = (a.start || '').toString();
    const bKey = (b.start || '').toString();
    return aKey.localeCompare(bKey);
  });

  dropdown.innerHTML = sorted.map(ev => {
    const dateIso = ev.start ? ev.start.toISOString().split('T')[0] : '';
    const dayName = ev.start ? ev.start.toLocaleDateString('he-IL', { weekday: 'short' }) : '';
    const d = ev.start ? ev.start.getDate() : '';
    const m = ev.start ? (ev.start.getMonth() + 1) : '';
    const y = ev.start ? String(ev.start.getFullYear()).slice(-2) : '';
    const dateShort = d + '.' + m + '.' + y;
    const timeStr = ev.start ? ev.start.toLocaleTimeString('he-IL', {
      hour: '2-digit', minute: '2-digit', hour12: false
    }) : '';
    return '<div class="cancel-req-item" data-date="' + dateIso + '" data-id="' + ev.id + '" data-source="' + (ev.extendedProps.source || 'firebase') + '" ' +
      'style="padding:6px 14px;border-bottom:1px solid #fecaca;cursor:pointer;display:flex;align-items:center;justify-content:space-between;font-size:.8rem;" ' +
      'onmouseenter="this.style.background=\'#fee2e2\'" onmouseleave="this.style.background=\'transparent\'">' +
      '<div style="display:flex;align-items:center;gap:6px;">' +
      '<span style="color:#dc3545;font-size:.7rem;">&#9679;</span>' +
      '<span style="font-weight:600;color:#991b1b;">' + ev.title + '</span>' +
      '</div>' +
      '<div style="display:flex;align-items:center;gap:6px;font-size:.78rem;color:#991b1b;">' +
      '<span style="font-weight:600;">' + dayName + '</span>' +
      '<span>' + dateShort + '</span>' +
      '<span style="font-weight:700;">' + timeStr + '</span>' +
      '<button onclick="event.stopPropagation();approveCancelReq(\'' + ev.id + '\',\'' + (ev.extendedProps.source || 'firebase') + '\')" ' +
        'style="background:#dc3545;color:#fff;border:none;border-radius:5px;padding:2px 8px;font-size:.72rem;font-weight:600;cursor:pointer;margin-right:4px;" ' +
        'title="אשר ביטול">&#10003; אשר</button>' +
      '<button onclick="event.stopPropagation();rejectCancelReq(\'' + ev.id + '\',\'' + (ev.extendedProps.source || 'firebase') + '\')" ' +
        'style="background:#fff;color:#991b1b;border:1px solid #dc3545;border-radius:5px;padding:2px 8px;font-size:.72rem;font-weight:600;cursor:pointer;" ' +
        'title="דחה בקשת ביטול">&#10007; דחה</button>' +
      '</div>' +
      '</div>';
  }).join('');

  // Wire up click handlers to navigate calendar to that date
  dropdown.querySelectorAll('.cancel-req-item').forEach(item => {
    item.addEventListener('click', function () {
      const dateStr = this.dataset.date;
      const evId = this.dataset.id;
      if (dateStr && calendar) {
        calendar.gotoDate(dateStr);
        calendar.changeView('timeGridDay', dateStr);
        _cancelDropdownOpen = false;
        dropdown.style.display = 'none';
        document.getElementById('cancelBannerArrow').style.transform = '';
        setTimeout(function () {
          var el = document.querySelector('[data-event-id="' + evId + '"]');
          if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
            el.style.transition = 'box-shadow .3s';
            el.style.boxShadow = '0 0 0 3px #dc3545, 0 0 12px rgba(220,53,69,.5)';
            setTimeout(function () { el.style.boxShadow = ''; }, 2000);
          }
        }, 300);
      }
    });
  });
}

function toggleCancelDropdown() {
  const dropdown = document.getElementById('cancelDropdown');
  const arrow = document.getElementById('cancelBannerArrow');
  _cancelDropdownOpen = !_cancelDropdownOpen;
  dropdown.style.display = _cancelDropdownOpen ? '' : 'none';
  arrow.style.transform = _cancelDropdownOpen ? 'rotate(180deg)' : '';
}

function approveCancelReq(id, source) {
  if (!confirm('לאשר את בקשת הביטול? התור יבוטל.')) return;
  fetch('/appointments/approve-cancel/' + id, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) _refetchEvents();
      else alert('שגיאה: ' + (data.error || 'לא ידוע'));
    })
    .catch(err => alert('שגיאת רשת: ' + err));
}

function rejectCancelReq(id, source) {
  if (!confirm('לדחות את בקשת הביטול? התור יישאר פעיל.')) return;
  fetch('/appointments/reject-cancel/' + id, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) _refetchEvents();
      else alert('שגיאה: ' + (data.error || 'לא ידוע'));
    })
    .catch(err => alert('שגיאת רשת: ' + err));
}
