import hashlib
import json

from fastapi import HTTPException

from . import recovery, surge
from .storage import audit


def find_hour(cur, query, *, lock=False):
    sql = 'SELECT * FROM floor_hours WHERE floor=%s AND hour_start=%s'
    cur.execute(sql + (' FOR UPDATE' if lock else ''), (query.floor, query.hour_start))
    return cur.fetchone()


def report_record(cur, hour_id, *, lock=False):
    cur.execute('SELECT r.*,p.username AS publisher,u.username AS recipient FROM floor_handoffs r '
                'JOIN users p ON p.id=r.published_by LEFT JOIN users u ON u.id=r.received_by WHERE r.hour_id=%s'
                + (' FOR UPDATE' if lock else ''), (hour_id,))
    record = cur.fetchone()
    if record:
        record['report'] = json.loads(record['report'])
    return record


def open_cases(cur, hour_id):
    cur.execute('SELECT c.id,c.store_id,c.status,c.action_slot,s.name,s.code, '
                '(SELECT COUNT(*) FROM recovery_checks k WHERE k.case_id=c.id) AS check_count, '
                '(SELECT MAX(k.slot_start) FROM recovery_checks k WHERE k.case_id=c.id) AS last_check_at '
                'FROM tenant_recoveries c JOIN stores s ON s.id=c.store_id '
                "WHERE c.hour_id=%s AND c.status <> 'closed' ORDER BY c.id", (hour_id,))
    return cur.fetchall()


def reconcile_digest(cases):
    # Any new case, closure or progress update (measures / follow-up checks) changes the digest,
    # which forces the next shift to re-check before signing.
    payload = [[row['id'], row['status'], row['check_count'],
                row['action_slot'].isoformat() if row['action_slot'] else None,
                row['last_check_at'].isoformat() if row['last_check_at'] else None] for row in cases]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def case_rows(cur, hour_id):
    return [{'id': row['id'], 'name': row['name'], 'code': row['code'], 'status': row['status'],
             'check_count': row['check_count'], 'last_check_at': row['last_check_at']}
            for row in open_cases(cur, hour_id)]


def snapshot(cur, hour_id):
    cur.execute('SELECT h.zone_number,s.name AS anchor,t.id AS task_id,t.status,t.cleared_queue,t.close_note '
                'FROM hotspots h JOIN stores s ON s.id=h.anchor_store_id LEFT JOIN dispatch_tasks t ON t.hotspot_id=h.id '
                'WHERE h.hour_id=%s ORDER BY h.zone_number', (hour_id,))
    zones = cur.fetchall()
    cur.execute('SELECT p.kind,p.windows,p.median_queue,s.name,s.code,c.status AS coverage_status,c.waiver_reason '
                'FROM analysis_points p JOIN stores s ON s.id=p.store_id '
                'LEFT JOIN coverage_orders c ON c.hour_id=p.hour_id AND c.store_id=p.store_id '
                'WHERE p.hour_id=%s ORDER BY s.code', (hour_id,))
    points = cur.fetchall()
    cur.execute('SELECT l.name AS lane_name,b.moved_count,b.task_id '
                'FROM lane_bookings b JOIN queue_lanes l ON l.id=b.lane_id '
                'JOIN dispatch_tasks t ON t.id=b.task_id JOIN hotspots h ON h.id=t.hotspot_id '
                "WHERE h.hour_id=%s AND b.status='released' ORDER BY b.id", (hour_id,))
    lane_uses = cur.fetchall()
    watch = [row | {'median_queue': float(row['median_queue']) if row['median_queue'] is not None else None}
             for row in points if row['kind'] in ('insufficient', 'noise')]
    responses = [{'id': row['id'], 'anchor': row['anchor'], 'status': row['status'],
                  'reported_queue': row['reported_queue'], 'release_queue': row['release_queue']}
                 for row in surge.for_hour(cur, hour_id)]
    followups = [{'id': row['id'], 'name': row['name'], 'status': row['status'], 'issue_note': row['issue_note'],
                  'checks': len(row['checks'])} for row in recovery.for_hour(cur, hour_id)]
    return {'surges': responses, 'recoveries': followups, 'entries': len(points), 'observed_windows': sum(row['windows'] for row in points),
            'missing_windows': sum(4 - row['windows'] for row in points), 'zones': zones, 'confirmed': sum(row['task_id'] is not None for row in zones), 'watch': watch,
            'lane_uses': lane_uses, 'moved': sum(row['moved_count'] for row in lane_uses),
            'pending': sum(row['status'] != 'closed' for row in zones)}


def latest_reconciliation(cur, handoff_id):
    cur.execute('SELECT * FROM handoff_reconciliations WHERE handoff_id=%s ORDER BY id DESC LIMIT 1', (handoff_id,))
    row = cur.fetchone()
    if row:
        row['open_cases'] = json.loads(row['open_cases'])
    return row


def transferred_cases(cur, handoff_id):
    cur.execute('SELECT g.case_id,g.owner_id,u.username AS owner,c.status,s.name,s.code '
                'FROM recovery_handoffs g JOIN tenant_recoveries c ON c.id=g.case_id '
                'JOIN stores s ON s.id=c.store_id JOIN users u ON u.id=g.owner_id '
                'WHERE g.handoff_id=%s ORDER BY g.case_id', (handoff_id,))
    return cur.fetchall()


def reconciliation_view(cur, record):
    hour_id = record['hour_id']
    current = open_cases(cur, hour_id)
    current_digest = reconcile_digest(current)
    check = latest_reconciliation(cur, record['id'])
    snapshot_ids = {row['id'] for row in record['report'].get('recoveries', [])}
    transferred = transferred_cases(cur, record['id'])
    for row in current:
        row['in_snapshot'] = row['id'] in snapshot_ids
        row['transferred'] = any(item['case_id'] == row['id'] for item in transferred)
    return {
        'open_cases': case_rows(cur, hour_id),
        'latest_check': {'at': check['checked_at'], 'by': check['checked_by'], 'digest': check['open_digest'],
                         'cases': check['open_cases']} if check else None,
        'stale': bool(check and check['open_digest'] != current_digest),
        'transferred': transferred,
        'new_after_publish': [row['id'] for row in current if row['id'] not in snapshot_ids],
        'closed_after_publish': sorted(snapshot_ids - {row['id'] for row in current}),
    }


def read_handoff(cur, query):
    hour = find_hour(cur, query)
    if not hour or hour['status'] != 'ready':
        return {'state': 'waiting_analysis', 'record': None, 'report': None}
    record = report_record(cur, hour['id'])
    if record:
        state = 'received' if record['received_by'] else 'published'
        return {'state': state, 'record': record, 'report': record['report'],
                'reconciliation': reconciliation_view(cur, record)}
    report = snapshot(cur, hour['id'])
    return {'state': 'waiting_dispatch' if report['pending'] else 'publishable', 'record': None, 'report': report}


def publish(cur, request, actor):
    hour = find_hour(cur, request, lock=True)
    if not hour or hour['status'] != 'ready':
        raise HTTPException(409, '请先完成该楼层小时的热区分析')
    if report_record(cur, hour['id']):
        raise HTTPException(409, '该楼层小时已发布交接简报')
    report = snapshot(cur, hour['id'])
    if report['pending']:
        raise HTTPException(409, '仍有热区待确认或疏导未结案，不能发布简报')
    cur.execute('INSERT INTO floor_handoffs(hour_id,report,published_by,publish_note) VALUES(%s,%s,%s,%s)',
                (hour['id'], json.dumps(report, ensure_ascii=False), actor, request.note))
    key = cur.lastrowid
    audit(cur, actor, 'handoff.published', key)
    return {'id': key, 'state': 'published'}


def reconcile(cur, key, actor):
    cur.execute('SELECT id,hour_id,received_by FROM floor_handoffs WHERE id=%s FOR UPDATE', (key,))
    record = cur.fetchone()
    if not record:
        raise HTTPException(404, '交接简报不存在')
    if record['received_by']:
        raise HTTPException(409, '该交接简报已签收，无需重复核对')
    cases = open_cases(cur, record['hour_id'])
    digest = reconcile_digest(cases)
    cur.execute('INSERT INTO handoff_reconciliations(handoff_id,open_digest,open_cases,checked_by) VALUES(%s,%s,%s,%s)',
                (key, digest, json.dumps(case_rows(cur, record['hour_id']), ensure_ascii=False, default=str), actor))
    check_id = cur.lastrowid
    audit(cur, actor, 'handoff.reconciled', check_id)
    return {'id': check_id, 'open_digest': digest, 'open_cases': case_rows(cur, record['hour_id'])}


def acknowledge(cur, key, note, actor):
    # Lock the handoff row so two stewards signing at the same time serialize here; only one can win.
    cur.execute('SELECT * FROM floor_handoffs WHERE id=%s FOR UPDATE', (key,))
    record = cur.fetchone()
    if not record:
        raise HTTPException(404, '交接简报不存在')
    if record['received_by']:
        raise HTTPException(409, '该交接简报已签收')
    check = latest_reconciliation(cur, key)
    if not check:
        raise HTTPException(409, '请先核对同楼层小时的未结工单再签收')
    current_digest = reconcile_digest(open_cases(cur, record['hour_id']))
    if check['open_digest'] != current_digest:
        raise HTTPException(409, '核对后工单有新单、结案或进度更新，请重新核对后再签收')
    cur.execute('UPDATE floor_handoffs SET received_by=%s,receive_note=%s,received_at=CURRENT_TIMESTAMP '
                'WHERE id=%s AND received_by IS NULL', (actor, note, key))
    if not cur.rowcount:
        # Concurrent sign-off: another steward won. Retrying must not create a second transfer.
        raise HTTPException(409, '该交接简报已由其他现场疏导员签收')
    # Sign-off and batch transfer take effect together: every still-open recovery case moves
    # to the recipient, who alone can register measures and follow-up checks afterwards.
    cur.execute("INSERT INTO recovery_handoffs(case_id,handoff_id,owner_id) "
                "SELECT c.id,%s,%s FROM tenant_recoveries c WHERE c.hour_id=%s AND c.status <> 'closed'",
                (key, actor, record['hour_id']))
    transferred = cur.rowcount
    audit(cur, actor, 'handoff.received', key)
    audit(cur, actor, 'handoff.transferred', key)
    return {'id': key, 'state': 'received', 'transferred': transferred}
