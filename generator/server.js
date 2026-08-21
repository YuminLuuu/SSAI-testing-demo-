#!/usr/bin/env node
/**
 * 「今天我该怎么办」原型 v0.3 的本地后端。
 *
 * 只做三件事：
 *   1. 把 generator/index.html 发出去
 *   2. /api/weather、/api/geocode —— 代实时天气请求，并归一成一个固定的形状
 *   3. /api/generate —— 代大模型请求，把流式结果转成页面能直接用的简单事件
 *
 * API key 只存在于这个进程的环境变量里，永远不进浏览器。
 * 零依赖，Node 18+ 即可（用内置 fetch）。
 *
 *   ANTHROPIC_API_KEY=sk-... node generator/server.js
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 5173;
const KEY = process.env.ANTHROPIC_API_KEY || '';
const MODEL = process.env.MODEL || 'claude-opus-5';
/* 演示模式：没有 key 时用假的流式输出跑通链路，方便离线演示 */
const STUB = process.env.STUB === '1';

const json = (res, code, body) => {
  res.writeHead(code, {'Content-Type': 'application/json; charset=utf-8'});
  res.end(JSON.stringify(body));
};

/* ---------------- 天气 ---------------- */
/* Open-Meteo：免费、无需 key。若改用和风天气等需要 key 的服务，
   只要在这里换掉 fetch 并保持返回的字段名不变，前端不用动。 */

const WMO = {
  0:'晴', 1:'多云', 2:'多云', 3:'阴', 45:'雾', 48:'雾凇',
  51:'毛毛雨', 53:'毛毛雨', 55:'毛毛雨', 61:'小雨', 63:'中雨', 65:'大雨',
  66:'冻雨', 67:'冻雨', 71:'小雪', 73:'中雪', 75:'大雪', 77:'雪粒',
  80:'阵雨', 81:'强阵雨', 82:'暴雨', 85:'阵雪', 86:'强阵雪',
  95:'雷阵雨', 96:'雷暴伴冰雹', 99:'强雷暴伴冰雹'
};

async function getWeather(lat, lon){
  const u = new URL('https://api.open-meteo.com/v1/forecast');
  u.searchParams.set('latitude', lat);
  u.searchParams.set('longitude', lon);
  u.searchParams.set('current', 'temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m');
  u.searchParams.set('daily', 'temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,weather_code');
  u.searchParams.set('timezone', 'auto');
  u.searchParams.set('forecast_days', '2');
  const r = await fetch(u);
  if(!r.ok) throw new Error('天气服务返回 ' + r.status);
  const d = await r.json();
  const c = d.current || {}, day = d.daily || {};
  const pick = (arr, i) => Array.isArray(arr) ? arr[i] : null;
  return {
    temp: c.temperature_2m, feels: c.apparent_temperature,
    humidity: c.relative_humidity_2m, precip: c.precipitation,
    wind: c.wind_speed_10m, code: c.weather_code,
    desc: WMO[c.weather_code] ?? '未知',
    today: {
      max: pick(day.temperature_2m_max,0), min: pick(day.temperature_2m_min,0),
      precipSum: pick(day.precipitation_sum,0), windMax: pick(day.wind_speed_10m_max,0)
    },
    tomorrow: {
      max: pick(day.temperature_2m_max,1), min: pick(day.temperature_2m_min,1),
      precipSum: pick(day.precipitation_sum,1), windMax: pick(day.wind_speed_10m_max,1),
      desc: WMO[pick(day.weather_code,1)] ?? '未知'
    },
    updatedAt: Date.now()
  };
}

async function geocode(q){
  const u = new URL('https://geocoding-api.open-meteo.com/v1/search');
  u.searchParams.set('name', q);
  u.searchParams.set('count', '6');
  u.searchParams.set('language', 'zh');
  u.searchParams.set('format', 'json');
  const r = await fetch(u);
  if(!r.ok) throw new Error('地名服务返回 ' + r.status);
  const d = await r.json();
  return (d.results || []).map(x => ({
    name: x.name, admin: [x.admin1, x.country].filter(Boolean).join(' · '),
    lat: x.latitude, lon: x.longitude
  }));
}

/* ---------------- 大模型 ---------------- */

/* 页面只认三种事件：text / done / error。
   Anthropic 那边的 SSE 结构在这里就地消化掉，浏览器不需要知道。 */
function sse(res){
  res.writeHead(200, {
    'Content-Type':'text/event-stream; charset=utf-8',
    'Cache-Control':'no-cache, no-transform',
    'Connection':'keep-alive'
  });
  return (obj)=>res.write('data: ' + JSON.stringify(obj) + '\n\n');
}

async function generate(req, res, body){
  const send = sse(res);
  if(STUB || !KEY){
    const demo = JSON.stringify(require('./stub.json'));
    for(let i=0;i<demo.length;i+=40){
      send({type:'text', text: demo.slice(i,i+40)});
      await new Promise(r=>setTimeout(r,12));
    }
    send({type:'done', stub:true}); return res.end();
  }
  try{
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        'x-api-key': KEY,
        'anthropic-version':'2023-06-01'
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 64000,
        stream: true,
        thinking: {type:'adaptive'},
        output_config: {effort:'high', format:{type:'json_schema', schema: body.schema}},
        system: body.system,
        messages: [{role:'user', content: body.prompt}]
      })
    });
    if(!r.ok){
      const t = await r.text();
      send({type:'error', message:`模型接口返回 ${r.status}：${t.slice(0,400)}`});
      return res.end();
    }
    /* 逐块读 SSE。只取 text_delta —— thinking_delta 不是给用户看的正文。 */
    const reader = r.body.getReader();
    const dec = new TextDecoder();
    let buf = '', textIdx = new Set();
    for(;;){
      const {done, value} = await reader.read();
      if(done) break;
      buf += dec.decode(value, {stream:true});
      const parts = buf.split('\n\n');
      buf = parts.pop();
      for(const part of parts){
        const line = part.split('\n').find(l=>l.startsWith('data:'));
        if(!line) continue;
        let ev; try{ ev = JSON.parse(line.slice(5).trim()); }catch{ continue; }
        if(ev.type==='content_block_start' && ev.content_block?.type==='text') textIdx.add(ev.index);
        if(ev.type==='content_block_delta' && ev.delta?.type==='text_delta' && textIdx.has(ev.index)){
          send({type:'text', text: ev.delta.text});
        }
        if(ev.type==='message_delta' && ev.delta?.stop_reason==='refusal'){
          send({type:'error', message:'模型拒绝了这次请求。请检查提示词内容。'});
        }
        if(ev.type==='error') send({type:'error', message: ev.error?.message || '模型接口出错'});
      }
    }
    send({type:'done'});
  }catch(e){
    send({type:'error', message: '请求失败：' + e.message});
  }
  res.end();
}

/* ---------------- 路由 ---------------- */

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if(req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  try{
    if(p === '/favicon.ico'){ res.writeHead(204); return res.end(); }

    if(p === '/api/health')
      return json(res, 200, {ok:true, llm: !!KEY || STUB, stub: STUB || !KEY, model: MODEL});

    if(p === '/api/geocode')
      return json(res, 200, {results: await geocode(url.searchParams.get('q') || '')});

    if(p === '/api/weather')
      return json(res, 200, await getWeather(url.searchParams.get('lat'), url.searchParams.get('lon')));

    if(p === '/api/generate' && req.method === 'POST'){
      let raw = '';
      req.on('data', c => raw += c);
      return req.on('end', () => {
        let body; try{ body = JSON.parse(raw); }catch{ return json(res,400,{error:'请求体不是合法 JSON'}); }
        generate(req, res, body);
      });
    }

    if(p === '/' || p === '/index.html'){
      const html = fs.readFileSync(path.join(__dirname, 'index.html'));
      res.writeHead(200, {'Content-Type':'text/html; charset=utf-8'});
      return res.end(html);
    }
    json(res, 404, {error:'not found'});
  }catch(e){
    json(res, 502, {error: e.message});
  }
});

server.listen(PORT, () => {
  console.log(`\n  原型运行在  http://localhost:${PORT}\n`);
  console.log(`  大模型：${KEY ? MODEL : '未配置 ANTHROPIC_API_KEY —— 走本地演示数据'}`);
  console.log(`  天气：  Open-Meteo（无需 key）\n`);
});
