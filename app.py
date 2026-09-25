import os, json, time, uuid, asyncio, secrets
from aiohttp import web, ClientSession, ClientWSTimeout, WSMsgType

PORT = int(os.getenv('PORT', '3000'))
API_WS = 'wss://developer.mig33.id/developer/ws'

sessions = {}
subscribers = {}
kick_executions = {}
kick_progress_subscribers = {}
balance_waiters = {}
message_waiters = {}
participant_waiters = {}


def make_id(): return secrets.token_hex(16)
def safe_error(err): return str(err) if err else 'Unknown error'
def now_ms(): return int(time.time() * 1000)


def extract_api_error(msg):
    data = msg.get('data') or {}
    code = str(data.get('code', data.get('error_code', data.get('error', msg.get('code', msg.get('error_code', ''))))))
    message = str(data.get('message', data.get('detail', data.get('error_message', msg.get('message', msg.get('error', 'Login failed'))))))
    return code.strip(), message.strip()


def classify_login_failure(err):
    text = str(err).lower()
    return 'suspend' if any(x in text for x in ('account suspended','user suspended','developer suspended','suspension','account blocked','account disabled','login blocked','login disabled')) else 'error'


def publish(session_id, msg):
    payload = f"data: {json.dumps(msg, separators=(',', ':'))}\n\n"
    for q in list(subscribers.get(session_id, set())):
        try: q.put_nowait(payload)
        except Exception: pass

async def close_session(session_id, reason='logout'):
    account = sessions.get(session_id)
    if not account: return False
    ping_task = account.get('pingTask')
    if ping_task and ping_task is not asyncio.current_task() and not ping_task.done():
        ping_task.cancel()
    ws = account.get('socket')
    if ws and not ws.closed:
        try: await ws.close()
        except Exception: pass
    client = account.get('client')
    if client and not client.closed:
        try: await client.close()
        except Exception: pass
    sessions.pop(session_id, None)
    for q in list(subscribers.pop(session_id, set())):
        try: q.put_nowait(None)
        except Exception: pass
    for table in (balance_waiters, participant_waiters, message_waiters):
        entry = table.pop(session_id, None)
        if entry:
            items = entry if isinstance(entry, list) else [entry]
            for fut in items:
                if not fut.done(): fut.set_exception(RuntimeError('Session ditutup sebelum respons diterima.'))
    return True


def is_vote_started(msg):
    data = msg.get('data') or msg
    return (str(msg.get('type', data.get('event_type', ''))).lower() == 'room.kick.state'
            and str(msg.get('action', data.get('action', ''))).lower() == 'vote_started'
            and 'vote to kick' in str(msg.get('status_message', data.get('status_message', ''))).lower())


def vote_key(msg):
    data = msg.get('data') or msg
    return '|'.join(str(data.get(k, '')).strip().lower() for k in ('room','target_username','username')) + '|' + str(data.get('time','')).strip() + '|' + str(data.get('action','')).strip().lower()


async def ws_reader(session_id, ws, username, socket_index, login_future):
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT: continue
            try: data = json.loads(msg.data)
            except Exception: continue
            resolve_waiters(session_id, data)
            received = now_ms()
            if socket_index == 0 and is_vote_started(data):
                account = sessions.get(session_id)
                if account:
                    account['countdownTrigger'] = {'key': vote_key(data), 'event': data, 'receivedAt': received}
                    publish(session_id, {'type':'countdown.trigger','socketIndex':0,'event':data,'receivedAt':received})
            publish(session_id, {'type':'api.event','socketIndex':socket_index,'event':data,'receivedAt':received})
            if data.get('type') == 'session.ready' and not login_future.done():
                account = {'sessionId':session_id,'username':username,'socket':ws,'connectedAt':now_ms(),'joinedRoom':None,'socketIndex':socket_index,'permissions':(data.get('data') or {}).get('developer',{}).get('permissions',[]),'countdownTrigger':None}
                sessions[session_id] = account
                if data.get('type') == 'session.ready':
                    wallet = (data.get('data') or {}).get('wallet') or (data.get('data') or {}).get('developer',{}).get('wallet')
                    login_future.set_result({'sessionId':session_id,'username':username,'permissions':account['permissions'],'wallet':wallet})
            if data.get('type') == 'room.join.result' and (data.get('data') or {}).get('room'):
                if session_id in sessions: sessions[session_id]['joinedRoom'] = data['data']['room']
            if data.get('type') == 'session.replaced': publish(session_id, {'type':'login.status','status':'error','code':'session.replaced','message':'Session digantikan oleh login lain.'})
            if data.get('type') == 'error' and not login_future.done():
                code, message = extract_api_error(data); login_future.set_exception(RuntimeError(f'{code}: {message}'))
    except Exception as e:
        if not login_future.done(): login_future.set_exception(e)
        publish(session_id, {'type':'session.error','error':safe_error(e)})
    finally:
        account = sessions.get(session_id)
        if account and account.get('socket') is ws:
            publish(session_id, {'type':'session.closed','reason':'WebSocket closed'})
            sessions.pop(session_id, None)
            client = account.get('client')
            if client and not client.closed:
                try: await client.close()
                except Exception: pass

async def ping_loop(session_id):
    while session_id in sessions:
        try:
            ws=sessions[session_id]['socket']
            if not ws.closed: await ws.send_str(json.dumps({'type':'ping'}))
            await asyncio.sleep(60)
        except Exception: break

async def connect_account(username, password, socket_index=None):
    session_id=make_id()
    client=ClientSession()
    try:
        ws=await client.ws_connect(API_WS, heartbeat=None, timeout=ClientWSTimeout(ws_close=15))
        await ws.send_str(json.dumps({'type':'developer.login','username':username,'password':password}))
        fut=asyncio.get_running_loop().create_future()
        asyncio.create_task(ws_reader(session_id,ws,username,socket_index,fut))
        result=await asyncio.wait_for(fut,15)
        if session_id in sessions:
            sessions[session_id]['client']=client
            sessions[session_id]['pingTask']=asyncio.create_task(ping_loop(session_id))
        else:
            await client.close()
            raise RuntimeError('WebSocket ditutup sebelum sesi siap.')
        return result
    except Exception:
        await client.close()
        raise

def send(session_id,payload):
    account=sessions.get(session_id)
    if not account or account['socket'].closed: raise RuntimeError('WebSocket tidak terhubung.')
    asyncio.create_task(account['socket'].send_str(json.dumps(payload)))


def new_waiter(table, session_id, timeout):
    loop=asyncio.get_running_loop(); fut=loop.create_future(); old=table.pop(session_id,None)
    if isinstance(old,list):
        for x in old:
            if not x.done(): x.set_exception(RuntimeError('Request digantikan.'))
    table[session_id]=fut
    async def timeout_task():
        await asyncio.sleep(timeout)
        if table.get(session_id) is fut:
            table.pop(session_id,None)
            if not fut.done(): fut.set_exception(asyncio.TimeoutError())
    asyncio.create_task(timeout_task()); return fut

def resolve_waiters(session_id,msg):
    typ=str(msg.get('type',''))
    if 'participant' in typ:
        fut=participant_waiters.pop(session_id,None)
        if fut and not fut.done(): fut.set_result(msg)
    if typ=='wallet.balance.result':
        fut=balance_waiters.pop(session_id,None)
        if fut and not fut.done(): fut.set_result((msg.get('data') or {}).get('wallet'))
    if typ in ('room.send_message.queued','error'):
        futs=message_waiters.pop(session_id,[])
        if not isinstance(futs,list): futs=[futs]
        for fut in futs:
            if not fut.done(): fut.set_exception(RuntimeError('room.send_message gagal.')) if typ=='error' else fut.set_result(msg)

async def sse_response(request, initial, queue):
    resp=web.StreamResponse(status=200,headers={'Content-Type':'text/event-stream','Cache-Control':'no-cache, no-transform','Connection':'keep-alive'})
    await resp.prepare(request); await resp.write((f'data: {json.dumps(initial)}\n\n').encode())
    try:
        while True:
            try: item=await asyncio.wait_for(queue.get(),20)
            except asyncio.TimeoutError: item=': keep-alive\n\n'
            if item is None: break
            await resp.write(item.encode())
    except (ConnectionResetError,asyncio.CancelledError): pass
    return resp

async def json_body(req):
    try:return await req.json()
    except:return {}

async def health(req): return web.json_response({'ok':True,'service':'MIG Duel Kick 10','activeSessions':len(sessions)})

async def login(req):
    b=await json_body(req); u=b.get('username'); p=b.get('password')
    if not u or not p:return web.json_response({'ok':False,'error':'Username dan password wajib diisi.'},status=400)
    try:return web.json_response({'ok':True,'account':await connect_account(str(u).strip(),str(p),0)})
    except Exception as e:return web.json_response({'ok':False,'status':classify_login_failure(e),'error':safe_error(e)},status=401)

async def login_batch(req):
    b=await json_body(req); inp=(b.get('accounts') or [])[:10]
    if not inp:return web.json_response({'ok':False,'error':'Tidak ada Troop untuk login.'},status=400)
    async def one(i,item):
        u=str(item.get('username','')).strip(); p=str(item.get('password','')); idx=item.get('index',i)
        if not u or not p:return {'index':idx,'ok':False,'error':'Nama dan password kosong.'}
        if item.get('sessionId'): await close_session(str(item['sessionId']),'relogin')
        try:return {'index':idx,'ok':True,'account':await connect_account(u,p,idx)}
        except Exception as e:return {'index':idx,'ok':False,'username':u,'status':classify_login_failure(e),'error':safe_error(e)}
    results=await asyncio.gather(*(one(i,x) for i,x in enumerate(inp))); return web.json_response({'ok':any(x['ok'] for x in results),'results':results})

async def action(req):
    b=await json_body(req); sid=b.get('sessionId'); act=b.get('action'); room=b.get('room'); target=b.get('targetUsername'); message=b.get('message')
    if not sid or not act:return web.json_response({'ok':False,'error':'Parameter tidak lengkap.'},status=400)
    try:
        if act=='join': send(sid,{'type':'room.join','room':room})
        elif act=='leave': send(sid,{'type':'room.leave','room':room})
        elif act=='participants':
            fut=new_waiter(participant_waiters,sid,8); send(sid,{'type':'room.participants','room':room}); return web.json_response({'ok':True,'sent':act,'event':await fut})
        elif act=='balance': send(sid,{'type':'wallet.balance'})
        elif act=='message': send(sid,{'type':'room.send_message','room':room,'message':message})
        elif act=='kick': send(sid,{'type':'room.kick','room':room,'target_username':target})
        else: raise RuntimeError('Action tidak dikenal.')
        return web.json_response({'ok':True,'sent':act})
    except Exception as e:return web.json_response({'ok':False,'error':safe_error(e)},status=400)

async def balance_all(req):
    b=await json_body(req); ids=list(dict.fromkeys(map(str,b.get('sessionIds',[]))))[:10]
    async def one(sid):
        try:
            fut=new_waiter(balance_waiters,sid,8); send(sid,{'type':'wallet.balance'}); return {'sessionId':sid,'ok':True,'wallet':await fut}
        except Exception as e:return {'sessionId':sid,'ok':False,'error':safe_error(e)}
    results=await asyncio.gather(*(one(x) for x in ids)); success=sum(x['ok'] for x in results)
    return web.json_response({'ok':success>0,'action':'balance','sent':len(ids),'success':success,'total':len(ids),'results':results})

async def batch_action(req):
    b=await json_body(req); ids=list(dict.fromkeys(map(str,b.get('sessionIds',[]))))[:10]; act=b.get('action'); room=b.get('room'); target=b.get('targetUsername'); msg=b.get('message')
    if not ids or not act:return web.json_response({'ok':False,'error':'Session atau action tidak lengkap.'},status=400)
    types={'join':'room.join','leave':'room.leave','participants':'room.participants','balance':'wallet.balance','kick':'room.kick','message':'room.send_message'}
    if act not in types:return web.json_response({'ok':False,'error':'Action tidak dikenal.'},status=400)
    payload={'type':types[act]};
    if act in ('join','leave','participants','kick','message'): payload['room']=room
    if act=='kick':payload['target_username']=target
    if act=='message':payload['message']=msg
    results=[]
    for sid in ids:
        try:send(sid,payload);results.append({'sessionId':sid,'ok':True})
        except Exception as e:results.append({'sessionId':sid,'ok':False,'error':safe_error(e)})
    return web.json_response({'ok':any(x['ok'] for x in results),'action':act,'sent':sum(x['ok'] for x in results),'total':len(results),'results':results})

async def kick_loop(req):
    b=await json_body(req); slots=b.get('websocketSlots') or []
    entries=sorted({int(x.get('websocket')):str(x.get('sessionId','')).strip() for x in slots if str(x.get('sessionId','')).strip() and 1<=int(x.get('websocket',0))<=10}.items())
    ws_entries=[{'websocket':k,'sessionId':v} for k,v in entries]
    if not ws_entries: ws_entries=[{'websocket':i+1,'sessionId':str(x)} for i,x in enumerate(b.get('sessionIds',[])[:10])]
    targets=[str(x).strip() for x in b.get('targets',[]) if str(x).strip()][:10]
    burst=max(1,min(int(b.get('burstSize',3) or 3),10)); target_delay=max(0,min(float(b.get('textdelay',0) or 0),86400000)); batch_delay=max(0,min(float(b.get('delayBatch',0) or 0),86400000)); loops=max(1,min(int(b.get('textloop',30) or 30),100))
    if not ws_entries or not targets:return web.json_response({'ok':False,'error':'Troop atau target kosong.'},status=400)
    total_steps=loops*len(targets); total_jobs=total_steps*len(ws_entries); eid=make_id()
    ex={'id':eid,'done':False,'result':None}; kick_executions[eid]=ex
    qset=set(); kick_progress_subscribers[eid]=qset
    state={'targetProgress':[{'targetIndex':i+1,'target':t,'completed':0,'dispatched':0,'total':len(ws_entries)*loops} for i,t in enumerate(targets)],'wsProgress':[{'websocket':x['websocket'],'sessionId':x['sessionId'],'completed':0,'dispatched':0,'total':total_steps,'failed':0} for x in ws_entries]}
    async def publish(ev):
        state['completedSteps']=min(total_steps,sum(x['completed'] for x in state['targetProgress'])); state['percent']=round(ev.get('dispatchedJobs',0)*100/total_jobs) if total_jobs else 0
        p={'type':'kick.progress',**ev,**state,'totalSteps':total_steps,'totalJobs':total_jobs}
        ex['latest']=p
        payload=f"data: {json.dumps({'ok':True,'executionId':eid,'done':ex['done'],'progress':p,'result':ex.get('result') if ex['done'] else None})}\n\n"
        for q in list(qset):
            try:q.put_nowait(payload)
            except:pass
    async def run(runtime,counters):
        sid=runtime['sessionId']; slot=runtime['websocket']; hist=[]
        for r in range(loops):
            for pos in range(0,len(targets),burst):
                group=targets[pos:pos+burst]
                for j,target in enumerate(group):
                    while True:
                        n=now_ms(); hist[:]=[t for t in hist if n-t<500]
                        if len(hist)<50: hist.append(n); break
                        await asyncio.sleep(max(.001,(500-(n-hist[0]))/1000))
                    try:
                        send(sid,{'type':'room.kick','room':b.get('room'),'target_username':target}); counters['dispatched']+=1; state['targetProgress'][pos+j]['dispatched']+=1; state['targetProgress'][pos+j]['completed']=min(state['targetProgress'][pos+j]['total'],state['targetProgress'][pos+j]['dispatched']//len(ws_entries)); state['wsProgress'][slot-1]['dispatched']+=1
                    except Exception: counters['failed']+=1; state['wsProgress'][slot-1]['failed']+=1
                    await publish({'phase':'dispatched','loop':r+1,'targetIndex':pos+j+1,'target':target,'websocket':slot,'sessionId':sid,'dispatchedJobs':counters['dispatched'],'failedJobs':counters['failed']})
                    if target_delay and j<len(group)-1: await asyncio.sleep(target_delay/1000)
                if batch_delay and pos+burst<len(targets): await asyncio.sleep(batch_delay/1000)
    async def runner():
        c={'dispatched':0,'failed':0}
        await publish({'phase':'started','dispatchedJobs':0,'failedJobs':0})
        try:
            await asyncio.gather(*(run(x,c) for x in ws_entries)); ex['done']=True; ex['result']={'dispatchedJobs':c['dispatched'],'failedJobs':c['failed']}; await publish({'phase':'completed' if not c['failed'] else 'completed_with_errors','dispatchedJobs':c['dispatched'],'failedJobs':c['failed']})
        except Exception as e: ex['done']=True; ex['result']={'error':safe_error(e)}; await publish({'phase':'error','error':safe_error(e),'dispatchedJobs':c['dispatched'],'failedJobs':c['failed']})
        await asyncio.sleep(600); kick_executions.pop(eid,None); kick_progress_subscribers.pop(eid,None)
    asyncio.create_task(runner())
    return web.json_response({'ok':True,'action':'kick-loop','executionId':eid,'mode':f'race_burst_{burst}_instant_dispatch','websockets':len(ws_entries),'targets':len(targets),'loops':loops,'totalSteps':total_steps,'totalJobs':total_jobs,'noAck':True})

async def kick_stream(req):
    eid=req.query.get('id',''); ex=kick_executions.get(eid)
    if not ex:return web.Response(status=404)
    q=asyncio.Queue(); kick_progress_subscribers.setdefault(eid,set()).add(q)
    resp=web.StreamResponse(headers={'Content-Type':'text/event-stream','Cache-Control':'no-cache, no-transform','Connection':'keep-alive'}); await resp.prepare(req)
    initial={'ok':True,'executionId':eid,'done':ex['done'],'progress':ex.get('latest',{}),'result':ex.get('result') if ex['done'] else None}; await resp.write((f'data: {json.dumps(initial)}\n\n').encode())
    try:
        while True:
            try:item=await asyncio.wait_for(q.get(),20)
            except asyncio.TimeoutError:item=': keep-alive\n\n'
            if item is None:break
            await resp.write(item.encode())
    except:pass
    kick_progress_subscribers.get(eid,set()).discard(q); return resp

async def kick_state(req):
    eid=req.query.get('id',''); ex=kick_executions.get(eid)
    if not ex:return web.json_response({'ok':False,'error':'Execution tidak ditemukan.'},status=404)
    return web.json_response({'ok':True,'executionId':eid,'done':ex['done'],'progress':ex.get('latest',{}),'result':ex.get('result') if ex['done'] else None})

async def events(req):
    sid=req.query.get('sessionId',''); account=sessions.get(sid)
    if not account:return web.Response(status=401)
    q=asyncio.Queue(); subscribers.setdefault(sid,set()).add(q); initial={'type':'stream.ready'}
    if account.get('socketIndex')==0 and account.get('countdownTrigger'): initial=account['countdownTrigger']
    return await sse_response(req,initial,q)

async def logout(req):
    b=await json_body(req); await close_session(str(b.get('sessionId','')),'logout'); return web.json_response({'ok':True})
async def logout_batch(req):
    b=await json_body(req); ids=list(dict.fromkeys(map(str,b.get('sessionIds',[]))))[:10] or list(sessions); closed=sum(await asyncio.gather(*(close_session(x,'logout all') for x in ids))); return web.json_response({'ok':True,'closed':closed})

app=web.Application(client_max_size=128*1024)
for path,handler,method in [('/api/health',health,'get'),('/api/login',login,'post'),('/api/login-batch',login_batch,'post'),('/api/action',action,'post'),('/api/balance-all',balance_all,'post'),('/api/batch-action',batch_action,'post'),('/api/kick-loop',kick_loop,'post'),('/api/kick-progress-stream',kick_stream,'get'),('/api/kick-progress-state',kick_state,'get'),('/api/events',events,'get'),('/api/logout',logout,'post'),('/api/logout-batch',logout_batch,'post')]: app.router.add_route(method,path,handler)
async def index(req):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), 'public', 'index.html'))

app.router.add_get('/', index)
app.router.add_static('/', os.path.join(os.path.dirname(__file__),'public'), show_index=False)

if __name__=='__main__': web.run_app(app,host='0.0.0.0',port=PORT)
