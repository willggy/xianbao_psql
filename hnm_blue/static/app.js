/* 海尼曼点读 — 前端逻辑 */
"use strict";
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

const API_BASE = '/hnm';   // 蓝图前缀: Flask 挂载在 /hnm

const API = {
  levels: () => fetch(`${API_BASE}/api/levels`).then(r => r.json()),
  books: lv => fetch(`${API_BASE}/api/books/${encodeURIComponent(lv)}`).then(r => r.json()),
  book: id => fetch(`${API_BASE}/api/book/${encodeURIComponent(id)}`).then(r => r.json()),
  quiz: id => fetch(`${API_BASE}/api/quiz/${encodeURIComponent(id)}`).then(r => r.json()),
  dict: id => fetch(`${API_BASE}/api/dict/${encodeURIComponent(id)}`).then(r => r.json()),
  razLevels: () => fetch(`${API_BASE}/api/raz_levels`).then(r => r.json()),
  razBooks: lv => fetch(`${API_BASE}/api/raz_books/${encodeURIComponent(lv)}`).then(r => r.json()),
  razBook: id => fetch(`${API_BASE}/api/raz_book/${encodeURIComponent(id)}`).then(r => r.json()),
};
const toast = (msg) => { const t = $("#toast"); t.textContent = msg; t.style.display = "block"; setTimeout(() => t.style.display = "none", 1800); };

/* 单例音频:复用同一个 Audio 元素,切 src 时旧解码缓冲被浏览器自动释放。
   避免长用时 new Audio 无限累积导致手机浏览器卡死。 */
let _audioEl = null;
let _audioSeq = 0;
function playAudio(url, rate = 1) {
  return new Promise((resolve) => {
    try {
      if (!_audioEl) { _audioEl = new Audio(); _audioEl.preload = "auto"; }
      const a = _audioEl;
      const seq = ++_audioSeq;
      a.onended = () => { if (a === _audioEl && seq === _audioSeq) resolve(true); };
      a.onerror = () => { console.warn("audio fail", url); if (a === _audioEl && seq === _audioSeq) resolve(false); };
      a.src = url;            // 切换 src:释放上一次的音频缓冲
      a.playbackRate = rate;
      a.play().catch(() => { console.warn("audio blocked", url); if (a === _audioEl && seq === _audioSeq) resolve(false); });
    } catch (e) { resolve(false); }
  });
}

/* ================= 路由 ================= */
const routes = {};
function go(name, ...args) {
  // 退出点读器时恢复页面滚动(routes.reader 设置了 body overflow:hidden)
  document.body.style.overflow = "";
  location.hash = "#/" + [name, ...args.map(encodeURIComponent)].join("/");
  // 切换路由时停止所有音频
  if (typeof stopAllAudio === 'function') stopAllAudio();
}
function route(fn) {
  const [name, ...args] = location.hash.replace(/^#\//, "").split("/").map(decodeURIComponent);
  (routes[name] || routes.home)(...args);
}
/* 统一的"返回书本"跳转: RAZ 书回 razBook 页, 海尼曼书回 book 页 */
function backBook() {
  if (window._bookBack) { const [rt, id] = window._bookBack; go(rt, id); }
  else go("book", window._lastEntry || "");
}
window.backBook = backBook;
window.addEventListener("hashchange", route);

/* ================= 首页: 级别选择 ================= */
let _curTab = "hnm";   // hnm | raz

function tabBar(active) {
  return `<div class="tabs">
    <div class="tab ${active === "hnm" ? "on" : ""}" onclick="switchTab('hnm')">海尼曼</div>
    <div class="tab ${active === "raz" ? "on" : ""}" onclick="switchTab('raz')">RAZ</div>
  </div>`;
}
function switchTab(t) {
  _curTab = t;
  // 直接渲染,不走 go('home')(hash 不变时 hashchange 不触发,页面不会刷新)
  if (typeof stopAllAudio === 'function') stopAllAudio();
  routes.home();
}
window.switchTab = switchTab;

routes.home = async () => {
  const title = _curTab === "raz" ? "RAZ 分级阅读" : "海尼曼分级阅读";
  $("#app").innerHTML = `${tabBar(_curTab)}<div class="topbar"><div class="title">${title}</div></div><div class="loading">加载中…</div>`;
  try {
    let levels, def;
    if (_curTab === "raz") {
      const d = await API.razLevels();
      levels = (d || []).map(x => ((x.title || {}).en || "").replace(/级$/, "")).filter(Boolean);
      def = "AA";
    } else {
      const d = await API.levels();
      levels = d.levels || [];
      def = d.default || "GK";
    }
    $("#app").innerHTML = `${tabBar(_curTab)}<div class="topbar"><div class="title">${title}</div><div class="sub">共 ${levels.length} 级</div></div>
      <div class="level-grid">
        ${levels.map(lv => `
          <div class="level-card ${lv === def ? "gk" : ""}" onclick="go('${_curTab === "raz" ? "razBooks" : "books"}','${esc(lv)}')">
            <div class="lv">${esc(lv)}</div>
            <div class="desc">${_curTab === "raz" ? "RAZ 分级读物" : (lv === "GK" ? "启蒙入门" : lv.startsWith("G") ? "进阶绘本" : "分级读物")}</div>
          </div>`).join("")}
      </div>`;
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= 书架 ================= */
routes.books = async (lv) => {
  window._lastLevel = lv;   // 记录来源级别,供 book 页返回
  $("#app").innerHTML = `${tabBar("hnm")}<div class="topbar"><button class="back" onclick="go('home')">‹</button><div class="title">${esc(lv)} 级</div></div><div class="loading">加载中…</div>`;
  try {
    const books = await API.books(lv);
    $("#app").innerHTML = `${tabBar("hnm")}<div class="topbar"><button class="back" onclick="go('home')">‹</button><div class="title">${esc(lv)} 级</div><div class="sub">${books.length} 本</div></div>
      <div class="shelf">
        ${books.map(b => {
          const id = (b.json || "").match(/\/entry\/id\/([^/]+)\/huiben/)?.[1] || "";
          const title = b.title?.en || "?";
          return `<div class="book-card" onclick="go('book','${esc(id)}')">
            <div class="book-cover">${b.cover ? `<img loading="lazy" src="${esc(b.cover)}" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">` : ""}<div class="noimg" style="display:${b.cover ? "none" : "flex"}">${esc(title)}</div></div>
            <div class="book-title">${esc(title)}</div>
          </div>`;
        }).join("")}
      </div>`;
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= RAZ 书架 ================= */
routes.razBooks = async (lv) => {
  window._lastLevel = lv;
  _curTab = "raz";
  $("#app").innerHTML = `${tabBar("raz")}<div class="topbar"><button class="back" onclick="go('home')">‹</button><div class="title">RAZ ${esc(lv)} 级</div></div><div class="loading">加载中…</div>`;
  try {
    const books = await API.razBooks(lv);
    $("#app").innerHTML = `${tabBar("raz")}<div class="topbar"><button class="back" onclick="go('home')">‹</button><div class="title">RAZ ${esc(lv)} 级</div><div class="sub">${books.length} 本</div></div>
      <div class="shelf">
        ${books.map(b => {
          const id = (b.json || "").match(/\/entry\/id\/([^/]+)\/ver\/3/)?.[1] || "";
          const title = b.title?.en || "?";
          return `<div class="book-card" onclick="go('razBook','${esc(id)}')">
            <div class="book-cover">${b.cover ? `<img loading="lazy" src="${esc(b.cover)}" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">` : ""}<div class="noimg" style="display:${b.cover ? "none" : "flex"}">${esc(title)}</div></div>
            <div class="book-title">${esc(title)}</div>
          </div>`;
        }).join("")}
      </div>`;
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= RAZ 书本: 阅读 + 重点单词 ================= */
routes.razBook = async (id) => {
  window._bookBack = ["razBook", id];   // reader/cards 返回目标
  _curTab = "raz";
  $("#app").innerHTML = `${tabBar("raz")}<div class="topbar"><button class="back" onclick="go('razBooks', window._lastLevel || 'AA')">‹</button><div class="title">加载中…</div></div>`;
  try {
    const d = await API.razBook(id);
    const title = d.title?.en || "书";
    const quiz = d.quiz || {};
    const pages = d.pages || [];
    const qid = (quiz.json || "").match(/id\/(\d+)/)?.[1] || quiz.quiz_id || "";
    $("#app").innerHTML = `${tabBar("raz")}<div class="topbar"><button class="back" onclick="go('razBooks', window._lastLevel || 'AA')">‹</button><div class="title">${esc(title)}</div></div>
      <div class="mod-list">
        <div class="mod-item" onclick="go('reader','${esc(id)}')">
          <div class="mod-icon read">📖</div>
          <div class="mod-body"><div class="mod-name">绘本阅读</div><div class="mod-desc">逐页点读 · ${pages.length} 页</div></div>
          <div class="mod-lock free">免费</div>
        </div>
        ${qid ? `<div class="mod-item" onclick="go('cards','${esc(qid)}','${esc(title)}','${esc(id)}')">
          <div class="mod-icon cards">★</div>
          <div class="mod-body"><div class="mod-name">重点单词</div><div class="mod-desc">看图识词 · 点击发音</div></div>
          <div class="mod-lock free">免费</div>
        </div>` : ""}
        ${qid ? `<div class="mod-item" onclick="go('words','${esc(qid)}','${esc(title)}','${esc(id)}')">
          <div class="mod-icon words">🔤</div>
          <div class="mod-body"><div class="mod-name">单词听选</div><div class="mod-desc">听发音选词 · 巩固重点单词</div></div>
          <div class="mod-lock free">免费</div>
        </div>` : ""}
      </div>`;
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= 书本: 四个模块 ================= */
routes.book = async (id) => {
  window._lastEntry = id;    // 记录海尼曼当前书,供 reader/cards 返回
  delete window._bookBack;   // 海尼曼书返回 book 页,不走 razBook
  $("#app").innerHTML = `<div class="topbar"><button class="back" onclick="go('books', window._lastLevel || 'GK')">‹</button><div class="title">加载中…</div></div>`;
  try {
    const d = await API.book(id);
    const title = d.title?.en || "书";
    const quiz = d.quiz || {};
    const gpc = d.gpc || {};
    const pages = d.pages || [];
    const qNeed = !!quiz.needPro, gNeed = !!gpc.needPro;
    const hasQuiz = quiz.json || quiz.quiz_id;
    const hasGpc = gpc.sentence || gpc.audio;
    const qid = (quiz.json || "").match(/id\/(\d+)/)?.[1] || quiz.quiz_id || "";
    $("#app").innerHTML = `
      <div class="topbar"><button class="back" onclick="go('books', window._lastLevel || 'GK')">‹</button><div class="title">${esc(title)}</div></div>
      <div class="mod-list">
        <div class="mod-item" onclick="go('reader','${esc(id)}')">
          <div class="mod-icon read">📖</div>
          <div class="mod-body"><div class="mod-name">绘本阅读</div><div class="mod-desc">逐页点读 · ${pages.length} 页</div></div>
          <div class="mod-lock free">免费</div>
        </div>
        ${hasQuiz ? `<div class="mod-item" onclick="go('cards','${esc(qid)}','${esc(title)}','${esc(id)}')">
          <div class="mod-icon cards">🃏</div>
          <div class="mod-body"><div class="mod-name">卡片练习</div><div class="mod-desc">看图识词</div></div>
          ${qNeed ? `<div class="mod-lock">会员</div>` : `<div class="mod-lock free">免费</div>`}
        </div>` : ""}
        ${hasQuiz ? `<div class="mod-item" onclick="go('words','${esc(qid)}','${esc(title)}','${esc(id)}')">
          <div class="mod-icon words">🔤</div>
          <div class="mod-body"><div class="mod-name">单词练习</div><div class="mod-desc">听音选词</div></div>
          ${qNeed ? `<div class="mod-lock">会员</div>` : `<div class="mod-lock free">免费</div>`}
        </div>` : ""}
        ${hasGpc ? `<div class="mod-item" onclick="go('sentence','${esc(id)}')">
          <div class="mod-icon sent">💬</div>
          <div class="mod-body"><div class="mod-name">句子练习</div><div class="mod-desc">高频句跟读</div></div>
          ${gNeed ? `<div class="mod-lock">会员</div>` : `<div class="mod-lock free">免费</div>`}
        </div>` : ""}
      </div>`;
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= 卡片练习 ================= */
routes.cards = async (qid, title, bookId) => {
  $("#app").innerHTML = `<div class="topbar"><button class="back" onclick="backBook()">‹</button><div class="title">${esc(title || "卡片练习")}</div></div><div class="loading">加载中…</div>`;
  try {
    const d = await API.quiz(qid);
    const items = Array.isArray(d) ? d : (d.data || []);
    const baseUrl = "https://books.diiiapp.com/words/b/";
    const soundUrl = "https://books.diiiapp.com/words/en/";
    let idx = 0;
    const show = (auto) => {
      const it = items[idx];
      $("#cardImg").src = baseUrl + it.image;
      $("#cardImg").onerror = () => { $("#cardImg").style.visibility = "hidden"; };
      $("#cardImg").style.visibility = "visible";
      $("#cardWord").textContent = it.title?.en || "";
      $("#cardIdx").textContent = `${idx + 1} / ${items.length}`;
      $("#prevBtn").disabled = idx === 0;
      $("#nextBtn").disabled = idx === items.length - 1;
      // 自动朗读: 翻卡后自动发音
      if (auto && autoReadOn) playAudio(soundUrl + it.voice.en, 1);
    };
    $("#app").innerHTML = `
      <div class="topbar"><button class="back" onclick="go('book','${esc(bookId)}')">‹</button><div class="title">${esc(title || "卡片练习")}</div></div>
      <div class="prac">
        <div class="card-area">
          <img id="cardImg" alt="">
          <div class="card-word" id="cardWord"></div>
          <div style="color:#bbb;font-size:13px;margin-top:10px" id="cardIdx"></div>
          <div class="prac-btns">
            <button id="prevBtn">‹ 上一张</button>
            <button id="playBtn" style="background:#ff6b1a">🔊 发音</button>
            <button id="nextBtn">下一张 ›</button>
          </div>
          <div class="auto-row">
            <button id="autoReadBtn" class="${autoReadOn ? "on" : ""}" onclick="toggleAutoRead()">自动朗读</button>
          </div>
          <div class="word-flow" id="wordFlow"></div>
        </div>
      </div>`;
    $("#playBtn").onclick = () => playAudio(soundUrl + items[idx].voice.en, 1);
    $("#prevBtn").onclick = () => { if (idx > 0) { idx--; show(true); } };
    $("#nextBtn").onclick = () => { if (idx < items.length - 1) { idx++; show(true); } };
    $("#wordFlow").innerHTML = items.map((it, i) =>
      `<div class="word-chip" data-i="${i}">${esc(it.title?.en || "")}</div>`).join("");
    $$("#wordFlow .word-chip").forEach(chip => chip.onclick = () => {
      idx = +chip.dataset.i; show(true);
    });
    show(false);
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= 单词练习 ================= */
routes.words = async (qid, title, bookId) => {
  $("#app").innerHTML = `<div class="topbar"><button class="back" onclick="go('book','${esc(bookId)}')">‹</button><div class="title">${esc(title || "单词练习")}</div></div><div class="loading">加载中…</div>`;
  try {
    const d = await API.quiz(qid);
    const items = Array.isArray(d) ? d : (d.data || []);
    const soundUrl = "https://books.diiiapp.com/words/en/";
    const baseUrl = "https://books.diiiapp.com/words/b/";
    let cur = 0, done = new Set();
    const showWord = () => {
      const it = items[cur];
      $("#wIdx").textContent = `${cur + 1} / ${items.length}`;
      $("#wWord").textContent = it.title?.en || "";
      $("#wImg").src = baseUrl + it.image;
      $("#wImg").onerror = () => { $("#wImg").style.visibility = "hidden"; };
      $("#wImg").style.visibility = "visible";
      $("#wPlay").disabled = false; $("#wPlay").textContent = "🔊 听发音";
      // 自动朗读: 显示新词自动播发音
      if (autoReadOn) playAudio(soundUrl + it.voice.en, 1);
    };
    $("#app").innerHTML = `
      <div class="topbar"><button class="back" onclick="go('book','${esc(bookId)}')">‹</button><div class="title">${esc(title || "单词练习")}</div></div>
      <div class="prac">
        <div class="card-area">
          <div style="color:#bbb;font-size:13px;margin-bottom:14px" id="wIdx"></div>
          <img id="wImg" alt="" style="max-width:200px;max-height:200px;margin-bottom:14px">
          <div class="card-word" id="wWord"></div>
          <div class="prac-btns">
            <button id="wPlay" style="background:#2f7df6">🔊 听发音</button>
          </div>
          <div class="auto-row">
            <button id="autoReadBtn" class="${autoReadOn ? "on" : ""}" onclick="toggleAutoRead()">自动朗读</button>
          </div>
          <p style="color:#999;font-size:13px;margin-top:16px">听发音，从下方选择正确的单词</p>
          <div class="word-flow" id="wOpts"></div>
        </div>
      </div>`;
    const optRender = () => {
      // 4 个选项: 正确 + 3 个随机干扰
      const correct = items[cur].title.en;
      const others = items.filter((x, i) => i !== cur && x.title.en !== correct);
      const pool = [correct];
      while (pool.length < 4 && others.length) {
        const pick = others.splice(Math.floor(Math.random() * others.length), 1)[0];
        if (!pool.includes(pick.title.en)) pool.push(pick.title.en);
      }
      const shuffled = pool.sort(() => Math.random() - 0.5);
      $("#wOpts").innerHTML = shuffled.map(w => `<div class="word-chip" data-w="${esc(w)}">${esc(w)}</div>`).join("");
      $$("#wOpts .word-chip").forEach(c => c.onclick = () => {
        if (c.dataset.w === correct) {
          c.classList.add("done"); toast("✅ 答对了!");
          playAudio(soundUrl + items[cur].voice.en, 1);
          setTimeout(() => { cur = (cur + 1) % items.length; showWord(); optRender(); }, 900);
        } else { c.classList.add("play"); toast("再试一次"); setTimeout(() => c.classList.remove("play"), 600); }
      });
    };
    $("#wPlay").onclick = () => playAudio(soundUrl + items[cur].voice.en, 1);
    showWord(); optRender();
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= 句子练习 ================= */
routes.sentence = async (id) => {
  $("#app").innerHTML = `<div class="topbar"><button class="back" onclick="go('book','${esc(id)}')">‹</button><div class="title">句子练习</div></div><div class="loading">加载中…</div>`;
  try {
    const d = await API.book(id);
    const gpc = d.gpc || {};
    const baseUrl = d.baseUrl || "";
    const sounds = gpc.sounds || [];
    const wordsMap = {};
    for (const s of sounds) { for (const k in s) { if (typeof s[k] === "string" && s[k]) wordsMap[k] = s[k]; } }
    $("#app").innerHTML = `
      <div class="topbar"><button class="back" onclick="go('book','${esc(id)}')">‹</button><div class="title">句子练习</div></div>
      <div class="prac">
        <div class="sentence-box">
          <div class="sentence-line" id="sLine"></div>
          <div class="sentence-zh" id="sZh"></div>
          <div class="prac-btns">
            <button id="sPlay" style="background:#9b51e0">🔊 整句朗读</button>
          </div>
          <div class="auto-row">
            <button id="autoReadBtn" class="${autoReadOn ? "on" : ""}" onclick="toggleAutoRead()">自动朗读</button>
          </div>
          <p style="color:#999;font-size:13px;margin-top:16px">点击句子中的单词可单独发音</p>
        </div>
      </div>`;
    const line = $("#sLine");
    // 用 text 构造词块: sentence 按空格分词,匹配 wordsMap
    const sent = gpc.sentence || "";
    const tokens = sent.split(/(\s+)/);
    line.innerHTML = tokens.map(t => {
      if (!t.trim()) return esc(t);
      const key = t.replace(/[.,!?;:""'()]/g, "");
      if (wordsMap[key]) return `<span class="w" data-k="${esc(key)}">${esc(t)}</span>`;
      return esc(t);
    }).join("");
    $$("#sLine .w").forEach(w => w.onclick = () => {
      const f = wordsMap[w.dataset.k];
      if (f && f.startsWith("ngk")) { playAudio(baseUrl + f, 1); }
      else if (f) { playAudio(f, 1); }
      w.classList.add("hit"); setTimeout(() => w.classList.remove("hit"), 500);
    });
    $("#sZh").textContent = gpc.translate || "";
    const playSentence = async () => {
      if (gpc.audio) {
        const url = gpc.audio.startsWith("http") ? gpc.audio : baseUrl + gpc.audio;
        await playAudio(url, 1);
      } else {
        for (const w of $$("#sLine .w")) { w.classList.add("hit"); await new Promise(r => setTimeout(r, 350)); w.classList.remove("hit"); }
      }
    };
    $("#sPlay").onclick = playSentence;
    // 自动朗读: 进入后自动朗读整句
    if (autoReadOn) playSentence();
  } catch (e) { $("#app").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`; }
};

/* ================= 点读器 ================= */
routes.reader = async (id) => {
  document.body.style.overflow = "hidden";
  window._lastEntry = id;
  $("#app").innerHTML = `<div class="reader" id="reader">
    <div class="reader-top">
      <button onclick="backBook()">‹ 返回</button>
      <div class="rt-title" id="rtTitle"></div>
      <button id="rtTrans" onclick="toggleTrans()">译</button>
      <button id="rtRate" onclick="toggleRate()">1.0x</button>
      <button id="rtAuto" class="${autoOn ? "on" : ""}" onclick="toggleAuto()">自动</button>
      <button id="rtFull" onclick="toggleFull()">全屏</button>
    </div>
    <div class="reader-stage" id="stage"></div>
    <div class="rate-panel" id="ratePanel">
      ${[0.5, 0.75, 1.0, 1.25, 1.5].map(r => `<button data-r="${r}" onclick="setRate(${r})">${r}x</button>`).join("")}
    </div>
    <div class="reader-nav">
      <button id="prevPg">‹ 上一页</button>
      <div class="page-ind" id="pgInd">1 / 1</div>
      <button id="nextPg">下一页 ›</button>
      <button class="primary" id="playPg">▶ 朗读</button>
      <button id="autoReadBtn" class="${autoReadOn ? "on" : ""}" onclick="toggleAutoRead()">自动朗读</button>
      <button id="backPg" onclick="backBook()">退出</button>
    </div>
  </div><div class="toast" id="toast"></div>`;
  try {
    const d = await API.book(id);
    window._book = d;
    window._pg = 0;
    window._rate = 1;
    window._auto = autoOn;
    window._trans = false;
    _huibenId = d.huiben_id || d.quiz?.heinemann_id || "";
    $("#rtTitle").textContent = d.title?.en || "";
    buildPage(0);
    $("#prevPg").onclick = () => flip(-1);
    $("#nextPg").onclick = () => flip(1);
    $("#playPg").onclick = () => playCurrent();
    $("#autoReadBtn").classList.toggle("on", autoReadOn);
    // 点空白关闭词义弹窗(document 级委托,每次进入 reader 只绑定一次)
    if (!window.__tipCloseBound) {
      window.__tipCloseBound = true;
      document.addEventListener("click", (ev) => {
        if (ev.target.closest && !ev.target.closest(".word-tip") && !ev.target.closest(".word")) {
          const tip = $("#wordTip");
          if (tip) tip.remove();
        }
      });
    }
    // 预取词典(整本书的单词释义),供点词显示
    if (_huibenId && !_dictCache[_huibenId]) {
      fetch(`${API_BASE}/api/dict/${_huibenId}`).then(r => r.json()).then(j => { _dictCache[_huibenId] = j; })
        .catch(() => {});
    }
    // 键盘翻页:模块级只绑定一次,避免重复监听累积
    if (!window.__keyBound) {
      window.__keyBound = true;
      document.addEventListener("keydown", k => {
        if (k.key === "ArrowRight") flip(1);
        if (k.key === "ArrowLeft") flip(-1);
      });
    }
  } catch (e) {
    $("#reader").innerHTML = `<div class="err">加载失败: ${esc(e.message)}</div>`;
  }
};
let autoOn = false;
let autoReadOn = false;
let _dictCache = {};      // huiben_id -> {tran: {word: 释义}}
let _huibenId = "";       // 当前书的 huiben_id,用于点词查词典

function buildPage(i) {
  const d = window._book;
  const pages = d.pages || [];
  if (i < 0 || i >= pages.length) return;
  window._pg = i;
  const pg = pages[i];
  const W = d.size?.w || 1920, H = d.size?.h || 960;
  const stage = $("#stage");
  const wrap = document.createElement("div");
  wrap.className = "page-wrap";
  wrap.style.width = "100%"; wrap.style.height = "100%";
  // 等比缩放适配舞台
  const rect = stage.getBoundingClientRect();
  const scale = Math.min(rect.width / W, rect.height / H);
  const vw = W * scale, vh = H * scale;
  wrap.style.width = vw + "px"; wrap.style.height = vh + "px";
  const baseUrl = d.baseUrl || "";
  // 图片层
  (pg.images || []).forEach(img => {
    const [x, y, w, h] = (img.rect || "0,0,0,0").split(",").map(Number);
    // RAZ 占位图 "@bg_white" / "@bg_xxx" = 纯色底,无实际图片文件,跳过
    const src = img.image || "";
    if (src.startsWith("@")) return;
    const el = document.createElement("img");
    el.className = "page-img";
    el.src = baseUrl + src;
    el.style.cssText = `left:${x * scale}px;top:${y * scale}px;width:${w * scale}px;height:${h * scale}px;`;
    wrap.appendChild(el);
  });
  // 文字层
  const midX = W / 2;   // 中线:文字不越过中线,避免压到右图
  (pg.texts || []).forEach(tx => {
    let [x, y, w, h] = (tx.rect || "0,0,0,0").split(",").map(Number);
    // 约束:宽度不超过中线
    if (x + w > midX - 20) w = Math.max(60, midX - 20 - x);
    const el = document.createElement("div");
    el.className = "page-text";
    el.style.cssText = `left:${x * scale}px;top:${y * scale}px;width:${w * scale}px;height:auto;min-height:${h * scale}px;`;
    el.style.color = tx.color ? "#" + tx.color : "#000";
    // 字体大小: font 形如 "LexendDeca-Regular,65.25"
    const fs = tx.font?.split(",")[1];
    el.style.fontSize = ((fs ? parseFloat(fs) : 40) * scale * 1.2) + "px";
    // words: [{'I':'ngk_1_en_i.mp3'}, {' ':0}, ...]
    const words = tx.words || [];
    if (words.length > 1) {
      // 词条:每个可点词是独立 span,词间用普通空格分隔(flex 折行时在词边界断,不拆词)
      const out = [];
      for (const w of words) {
        for (const k in w) {
          const v = w[k];
          const txt = (k === "" ? "" : k);
          if (v && typeof v === "string") {
            out.push(`<span class="word" data-w="${esc(txt)}" data-f="${esc(v)}">${esc(txt)}</span>`);
          } else if (txt.trim() === "") {
            out.push("<span class=\"gap\">&nbsp;</span>");   // 词间空格:独占一个可断行位
          } else {
            out.push(esc(txt));                               // 标点等
          }
          out.push(txt.trim() ? "" : "");                     // no-op; 分隔交给 span
        }
      }
      el.innerHTML = out.join("");
      $$(".word", el).forEach(sp => sp.onclick = () => {
        sp.classList.add("hit");
        const w = sp.dataset.w || "";
        playAudio(baseUrl + sp.dataset.f, window._rate || 1);
        showWordTip(sp, w);
        setTimeout(() => sp.classList.remove("hit"), 600);
      });
    } else {
      el.textContent = tx.text || "";
    }
    wrap.appendChild(el);
  });
  // 清空舞台,避免每次 buildPage 叠加旧 wrap
  stage.innerHTML = "";
  // 中译:显示在书本左下方(默认关闭,点"译"开启) — 挂在 wrap 内,跟随白色书本定位
  if (window._trans && pg.translate) {
    const t = document.createElement("div");
    t.className = "page-translate"; t.textContent = pg.translate;
    wrap.appendChild(t);
  }
  stage.appendChild(wrap);
  $("#pgInd").textContent = `${i + 1} / ${pages.length}`;
}

/* 点词弹窗: 发音 + 词典释义 */
function showWordTip(sp, word) {
  const old = $("#wordTip");
  if (old) old.remove();
  const dict = _dictCache[_huibenId]?.tran || {};
  const zh = dict[word.toLowerCase()] || "";
  const tip = document.createElement("div");
  tip.id = "wordTip";
  tip.className = "word-tip";
  const r = sp.getBoundingClientRect();
  const stageRect = $("#stage").getBoundingClientRect();
  let left = r.left - stageRect.left;
  let top = r.bottom - stageRect.top + 8;
  if (left + 220 > stageRect.width) left = stageRect.width - 230;
  tip.style.left = left + "px";
  tip.style.top = top + "px";
  tip.innerHTML = `
    <div class="wt-head">
      <span class="wt-word">${esc(word)}</span>
      <span class="wt-spk" onclick="playWordSound('${esc(word)}')">🔊</span>
    </div>
    <div class="wt-zh">${esc(zh || "暂无释义")}</div>`;
  stage.appendChild(tip);
  setTimeout(() => tip.classList.add("show"), 10);
}
function playWordSound(word) {
  // 用当前页 words 里的音频文件:从当前文字节点找
  const sp = [...$$("#stage .word")].find(x => x.dataset.w?.toLowerCase() === word.toLowerCase());
  if (sp?.dataset.f) playAudio((window._book?.baseUrl || "") + sp.dataset.f, window._rate || 1);
}

function stopAllAudio() {
  // 单例音频:暂停并清空 src,释放解码缓冲
  if (_audioEl) {
    try { _audioEl.pause(); _audioEl.removeAttribute("src"); _audioEl.load(); } catch (e) {}
  }
  // 兼容旧 DOM 音频(若有)
  document.querySelectorAll("audio").forEach(a => {
    try { a.pause(); a.removeAttribute("src"); a.load(); } catch (e) {}
  });
}

function flip(delta) {
  const pages = window._book?.pages || [];
  const n = window._pg + delta;
  if (n < 0 || n >= pages.length) { toast(delta > 0 ? "已经是最后一页" : "已经是第一页"); return; }
  // 翻页时停止所有正在播放的音频
  stopAllAudio();
  buildPage(n);
  // 需求1: 开了"自动朗读"则翻页后自动朗读当前页
  if (autoReadOn) playCurrent();
}

function toggleAutoRead() {
  autoReadOn = !autoReadOn;
  const b = $("#autoReadBtn");
  if (b) b.classList.toggle("on", autoReadOn);
  toast(autoReadOn ? "自动朗读已开启(切换时自动朗读)" : "自动朗读已关闭");
}

async function playCurrent() {
  const pg = window._book?.pages?.[window._pg];
  if (!pg) return;
  const btn = $("#playPg");
  btn.disabled = true;
  const baseUrl = window._book.baseUrl || "";
  for (const s of pg.auto_sounds || []) {
    await playAudio(baseUrl + s.file, window._rate);
  }
  btn.disabled = false;
}

function toggleTrans() {
  window._trans = !window._trans;
  $("#rtTrans").classList.toggle("on", window._trans);
  buildPage(window._pg);
}
function toggleRate() {
  $("#ratePanel").classList.toggle("show");
}
function setRate(r) {
  window._rate = r;
  $("#rtRate").textContent = r + "x";
  $("#ratePanel").classList.remove("show");
  $$("#ratePanel button").forEach(b => b.classList.toggle("on", +b.dataset.r === r));
}
function toggleAuto() {
  autoOn = !autoOn;
  $("#rtAuto").classList.toggle("on", autoOn);
  if (autoOn) autoPlayLoop();
}
async function autoPlayLoop() {
  while (autoOn) {
    const pg = window._book?.pages?.[window._pg];
    if (!pg) break;
    const btn = $("#playPg");
    const baseUrl = window._book.baseUrl || "";
    for (const s of pg.auto_sounds || []) {
      if (!autoOn) return;
      await playAudio(baseUrl + s.file, window._rate);
    }
    if (!autoOn) return;
    await new Promise(r => setTimeout(r, 900));
    if (window._pg < window._book.pages.length - 1) flip(1);
    else { autoOn = false; $("#rtAuto").classList.remove("on"); break; }
  }
}
function toggleFull() {
  if (!document.fullscreenElement) document.documentElement.requestFullscreen?.();
  else document.exitFullscreen?.();
}

/* ================= 启动 ================= */
window.go = go; window.toggleTrans = toggleTrans; window.toggleRate = toggleRate;
window.setRate = setRate; window.toggleAuto = toggleAuto; window.toggleFull = toggleFull;
window.toggleAutoRead = toggleAutoRead; window.playWordSound = playWordSound;
document.addEventListener("DOMContentLoaded", route);
window.addEventListener("resize", () => { if (window._book) buildPage(window._pg); });
