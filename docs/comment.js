class App {
    constructor() {
        this.videoList = null;
        this.videos = [];
        this.filteredVideos = [];
        this.currentPage = 0;
        this.pageSize = 12;
        this.currentSort = 'time-desc';
        this.selectedYear = '';
        this.baseUrl = '';
        this.commentsCache = {};
        this.searchResults = [];
        this.searchPage = 0;
        this.searchPageSize = 20;
        this.currentComments = [];
        this.commentsPage = 0;
        this.commentsPageSize = 35;
        this.currentVideo = null;
        this.isSearching = false;
        this.currentSecUid = '';
        this.allUsers = [];
        this.emojiMap = {};
        this.emojiLoaded = false;
        this.slider = { images: [], index: 0 };
        this.init();
    }
    
    // ==================== 工具方法 ====================
    
    getFullUrl(path) {
        if (!path) return '';
        if (/^(https?:\/\/|\/)/.test(path)) return path;
        return this.baseUrl + path;
    }
    
    getPlaceholderSvg(type) {
        const svgs = {
            thumb: `data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><rect fill=%22%23e0e0e0%22 width=%22100%22 height=%22100%22/><text x=%2250%22 y=%2250%22 text-anchor=%22middle%22 dy=%22.3em%22 fill=%22%23999%22>无图片</text></svg>`,
            avatar: `data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 28 28%22><rect fill=%22%23ddd%22 width=%2228%22 height=%2228%22/><text x=%2214%22 y=%2214%22 text-anchor=%22middle%22 dy=%22.3em%22 fill=%22%23999%22 font-size=%2210%22>?</text></svg>`
        };
        return svgs[type] || '';
    }
    
    getMainUrls(arr) {
        if (!arr || arr.length === 0) return [];
        if (Array.isArray(arr[0])) return arr.map(p => p[0] || '').filter(Boolean);
        return arr.filter(Boolean);
    }
    
    formatDateTime(video) {
        if (video.create_time) {
            const d = new Date(video.create_time * 1000);
            return `${String(d.getMonth()+1).padStart(2,'0')}月${String(d.getDate()).padStart(2,'0')}日 ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
        }
        if (video.create_time_str) {
            const parts = video.create_time_str.split(' ');
            if (parts.length === 2) {
                const dp = parts[0].split('-');
                if (dp.length === 3) return `${dp[1]}/${dp[2]} ${parts[1]}`;
            }
            return video.create_time_str;
        }
        return '';
    }
    
    formatTimestamp(ts) {
        if (!ts) return '';
        const d = new Date(parseInt(ts) * 1000);
        return `${String(d.getMonth()+1).padStart(2,'0')}月${String(d.getDate()).padStart(2,'0')}日 ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
    }
    
    formatNumber(num) {
        if (!num) return '0';
        num = parseInt(num);
        if (num >= 10000) {
            const w = num / 10000;
            return w >= 10 ? `${Math.round(w)}万` : `${w.toFixed(1).replace(/\.0$/,'')}万`;
        }
        return num.toString();
    }
    
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    escapeAttr(str) {
        if (!str) return '';
        return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    }
    
    // ==================== 表情 ====================
    
    async loadEmojiMap() {
        try {
            const resp = await fetch('emoji.json');
            const data = await resp.json();
            this.emojiMap = {};
            for (const e of data.emoji_list || []) {
                const name = e.display_name, url = e.emoji_url?.url_list?.[0] || '';
                if (name && url) this.emojiMap[name] = url;
            }
            this.emojiLoaded = true;
        } catch (err) { console.error('加载表情映射失败:', err); this.emojiLoaded = false; }
    }
    
    replaceTextEmojis(text) {
        if (!text || !this.emojiLoaded) return text;
        let result = this.escapeHtml(text);
        for (const [name, url] of Object.entries(this.emojiMap)) {
            const esc = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            result = result.replace(new RegExp(esc, 'g'), `<img class="comment-emoji" src="${url}" alt="${name}" title="${name}">`);
        }
        return result;
    }
    
    // ==================== 统一内容渲染 ====================
    
    renderContentHtml(item) {
        let html = '';
        if (item.text) html += this.replaceTextEmojis(item.text);
        if (item.sticker) html += `<img class="comment-sticker" src="${this.getFullUrl(item.sticker)}" alt="表情" loading="lazy">`;
        if (item.image_list?.length > 0) {
            const mainUrls = this.getMainUrls(item.image_list);
            if (mainUrls.length) {
                const urlsJson = JSON.stringify(mainUrls.map(u => this.getFullUrl(u)));
                html += `<div class="comment-images">` +
                    mainUrls.map((url, i) => `<img class="comment-image" src="${this.getFullUrl(url)}" alt="图片${i+1}" loading="lazy" data-urls='${this.escapeAttr(urlsJson)}' data-index="${i}" onclick="app.openSliderFromEl(this)">`).join('') +
                    `</div>`;
            }
        }
        return html || '<span class="empty-text">[信息为图片或表情 系统未保存]</span>';
    }
    
    renderUserHeader(item, cssPrefix, avatarPlaceholder, replyTo = '') {
        const avatarHtml = item.user_avatar
            ? `<img class="${cssPrefix}-avatar" src="${this.getFullUrl(item.user_avatar)}" alt="头像" onerror="this.src='${avatarPlaceholder}'">`
            : '';
        const ipHtml = item.ip_label ? `<span class="${cssPrefix}-ip">${item.ip_label}</span>` : '';
        const replyToHtml = replyTo ? `<span class="reply-to">回复 @<span data-copy="${this.escapeAttr(replyTo)}">${this.escapeHtml(replyTo)}</span></span>` : '';
        return `
            ${avatarHtml}
            <div class="${cssPrefix}-user">
                <div class="${cssPrefix}-user-row">
                    <span class="${cssPrefix}-user-info">
                        <span class="${cssPrefix}-nickname" data-copy="${this.escapeAttr(item.user_nickname || '匿名')}">${this.escapeHtml(item.user_nickname || '匿名')}</span>
                        ${ipHtml}
                        ${replyToHtml}
                    </span>
                </div>
                <span class="${cssPrefix}-time">${this.formatTimestamp(item.create_time)}</span>
            </div>`;
    }
    
    // ==================== 统一图片滑动器 ====================
    
    openSlider(images, index) {
        this.slider = { images, index };
        const ph = this.getPlaceholderSvg('thumb');
        let viewer = document.getElementById('image-viewer');
        if (!viewer) {
            viewer = document.createElement('div');
            viewer.id = 'image-viewer';
            viewer.className = 'image-viewer';
            viewer.innerHTML = `
                <button class="image-viewer-close" onclick="event.stopPropagation();app.closeSlider()">&times;</button>
                <button class="image-nav prev" onclick="event.stopPropagation();app.sliderNav(-1)"></button>
                <img id="viewer-image" src="" alt="预览" onerror="this.src='${ph}'" onclick="event.stopPropagation();app.closeSlider()">
                <button class="image-nav next" onclick="event.stopPropagation();app.sliderNav(1)"></button>`;
            viewer.addEventListener('click', () => this.closeSlider());
            document.body.appendChild(viewer);
        }
        const img = document.getElementById('viewer-image');
        img.src = images[index];
        img.onerror = () => { img.src = ph; };
        viewer.classList.add('active');
        const hasMul = images.length > 1;
        viewer.querySelector('.image-nav.prev').style.display = hasMul ? 'flex' : 'none';
        viewer.querySelector('.image-nav.next').style.display = hasMul ? 'flex' : 'none';
    }
    
    closeSlider() { document.getElementById('image-viewer')?.classList.remove('active'); }
    
    sliderNav(delta) {
        const s = this.slider;
        s.index = (s.index + delta + s.images.length) % s.images.length;
        const img = document.getElementById('viewer-image');
        img.src = s.images[s.index];
        img.onerror = () => { img.src = this.getPlaceholderSvg('thumb'); };
    }
    
    openSliderFromEl(el) {
        this.openSlider(JSON.parse(el.dataset.urls || '[]'), parseInt(el.dataset.index || '0'));
    }
    
    carouselNav(delta) {
        if (!this.slider.images.length) return;
        this.slider.index = (this.slider.index + delta + this.slider.images.length) % this.slider.images.length;
        this.updateCarousel();
    }
    
    carouselGoTo(i) { this.slider.index = i; this.updateCarousel(); }
    
    updateCarousel() {
        const c = document.getElementById('carousel-container');
        if (c) c.style.transform = `translateX(-${this.slider.index * 100}%)`;
        document.querySelectorAll('.carousel-dot').forEach((d, i) => d.classList.toggle('active', i === this.slider.index));
    }
    
    // ==================== 分页 ====================
    
    createPaginationHtml(curPage, totalPages, total, tpl) {
        if (totalPages <= 1) return '';
        let html = `<div class="pagination-info">第 ${curPage+1}/${totalPages} 页 (共 ${total} 条)</div><div class="pagination-btns">`;
        if (curPage > 0) html += `<button class="page-btn" onclick="app.${tpl.replace(/\$\{page\}/g, curPage-1)}">上一页</button>`;
        let s = Math.max(0, curPage - 2), e = Math.min(totalPages-1, s + 4);
        if (e - s < 4) s = Math.max(0, e - 4);
        for (let i = s; i <= e; i++) {
            html += i === curPage
                ? `<button class="page-btn active">${i+1}</button>`
                : `<button class="page-btn" onclick="app.${tpl.replace(/\$\{page\}/g, i)}">${i+1}</button>`;
        }
        if (curPage < totalPages-1) html += `<button class="page-btn" onclick="app.${tpl.replace(/\$\{page\}/g, curPage+1)}">下一页</button>`;
        return html + '</div>';
    }
    
    // ==================== 初始化 ====================
    
    setUserHeader(user) {
        const n = user.nickname || '用户';
        document.title = n;
        document.getElementById('header-title').textContent = n;
        const url = `data/comment/${user.sec_uid}/avatar.jpg`;
        const el = document.getElementById('header-avatar');
        el.src = url;
        const fav = document.getElementById('favicon');
        fav.href = url; fav.type = 'image/jpeg';
        el.onerror = () => { el.src = this.getPlaceholderSvg('avatar'); };
    }
    
    bindEvents() {
        document.getElementById('search-btn').addEventListener('click', () => this.search());
        document.getElementById('search-input').addEventListener('keypress', e => { if (e.key === 'Enter') this.search(); });
        document.getElementById('search-close-btn').addEventListener('click', () => this.closeSearchResults());
        this.initCustomSelects();
        document.getElementById('load-more-btn').addEventListener('click', () => this.loadMore());
        document.getElementById('modal-close').addEventListener('click', () => this.closeModal());
        document.getElementById('video-modal').addEventListener('click', e => { if (e.target.id === 'video-modal') this.closeModal(); });
        document.addEventListener('keydown', e => {
            if (e.key === 'Escape') { this.closeModal(); this.closeSearchResults(); this.closeAllCustomSelects(); }
        });
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                document.querySelectorAll('#video-modal video, #video-modal audio').forEach(el => {
                    el.pause();
                });
            }
        });
        document.addEventListener('click', e => {
            const copyable = e.target.closest('[data-copy]');
            if (copyable) { this.copyToClipboard(copyable.dataset.copy, copyable); return; }
            if (!e.target.closest('.custom-select')) this.closeAllCustomSelects();
        });
    }
    
    initCustomSelects() {
        document.querySelectorAll('.custom-select').forEach(sel => {
            const trigger = sel.querySelector('.custom-select-trigger');
            trigger.addEventListener('click', e => {
                e.stopPropagation();
                const isOpen = sel.classList.contains('open');
                this.closeAllCustomSelects();
                if (!isOpen) sel.classList.add('open');
            });
            sel.querySelectorAll('.custom-select-option').forEach(opt => {
                opt.addEventListener('click', () => {
                    sel.querySelectorAll('.custom-select-option').forEach(o => o.classList.remove('active'));
                    opt.classList.add('active');
                    trigger.querySelector('span').textContent = opt.textContent;
                    trigger.dataset.value = opt.dataset.value;
                    sel.classList.remove('open');
                    this.handleSelectChange(sel.id, opt.dataset.value);
                });
            });
        });
    }
    
    closeAllCustomSelects() { document.querySelectorAll('.custom-select').forEach(s => s.classList.remove('open')); }
    
    handleSelectChange(id, val) {
        if (id === 'search-type-wrapper') return;
        if (id === 'sort-select-wrapper') { this.currentSort = val; this.sortVideos(); this.renderVideos(true); }
        if (id === 'year-select-wrapper') { this.selectedYear = val; this.applyFilters(); }
    }
    
    async loadData() {
        try {
            const idx = await (await fetch('data/comment/index.json')).json();
            if (!idx.users?.length) throw new Error('没有找到用户数据');
            
            this.allUsers = idx.users;
            
            const savedSecUid = this.getPreferredUser();
            const user = idx.users.find(u => u.sec_uid === savedSecUid) || idx.users[0];
            
            this.initUserSwitcher();
            await this.loadUserData(user.sec_uid);
            
            document.getElementById('loading').style.display = 'none';
            document.getElementById('video-grid').style.display = 'grid';
        } catch (err) {
            document.getElementById('loading').innerHTML = `<p style="color:var(--primary-color)">加载数据失败</p><p style="color:var(--text-secondary);margin-top:10px">${err.message}</p>`;
        }
    }
    
    getPreferredUser() {
        const params = new URLSearchParams(window.location.search);
        if (params.has('user')) return params.get('user');
        return localStorage.getItem('preferred_user') || '';
    }
    
    initUserSwitcher() {
        const switchBtn = document.getElementById('user-switch-btn');
        
        if (!switchBtn || this.allUsers.length <= 1) {
            if (switchBtn) switchBtn.style.display = 'none';
            return;
        }
        
        switchBtn.style.display = 'inline-flex';
        switchBtn.replaceWith(switchBtn.cloneNode(true));
        
        document.getElementById('user-switch-btn').addEventListener('click', () => {
            this.showUserSwitchModal();
        });
        
        document.getElementById('user-switch-close').addEventListener('click', () => {
            this.hideUserSwitchModal();
        });
        
        document.getElementById('user-switch-overlay').addEventListener('click', () => {
            this.hideUserSwitchModal();
        });
    }
    
    showUserSwitchModal() {
        const modal = document.getElementById('user-switch-modal');
        const list = document.getElementById('user-list');
        
        list.innerHTML = this.allUsers.map(u => {
            const isActive = u.sec_uid === this.currentSecUid;
            return `
                <div class="user-list-item ${isActive ? 'active' : ''}" data-sec-uid="${u.sec_uid}">
                    <img class="user-list-avatar" src="data/comment/${u.sec_uid}/avatar.jpg" alt="${u.nickname}" onerror="this.src='${this.getPlaceholderSvg('avatar')}'">
                    <div class="user-list-info">
                        <div class="user-list-name">${this.escapeHtml(u.nickname || u.sec_uid.substring(0, 20))}</div>
                        <div class="user-list-stats">
                            <span>${u.total_videos || 0} 作品</span>
                            <span>${u.total_comments || 0} 评论</span>
                        </div>
                    </div>
                    ${isActive ? '<div class="user-list-check">✓</div>' : ''}
                </div>
            `;
        }).join('');
        
        list.querySelectorAll('.user-list-item').forEach(el => {
            el.addEventListener('click', () => {
                const secUid = el.dataset.secUid;
                if (secUid !== this.currentSecUid) {
                    this.switchUser(secUid);
                }
                this.hideUserSwitchModal();
            });
        });
        
        modal.style.display = 'block';
        setTimeout(() => modal.classList.add('active'), 10);
    }
    
    hideUserSwitchModal() {
        const modal = document.getElementById('user-switch-modal');
        modal.classList.remove('active');
        setTimeout(() => modal.style.display = 'none', 200);
    }
    
    async switchUser(secUid) {
        if (secUid === this.currentSecUid) return;
        
        document.getElementById('loading').style.display = 'block';
        document.getElementById('video-grid').style.display = 'none';
        this.closeSearchResults();
        
        try {
            await this.loadUserData(secUid);
            document.getElementById('loading').style.display = 'none';
            document.getElementById('video-grid').style.display = 'grid';
            
            const url = new URL(window.location);
            url.searchParams.set('user', secUid);
            window.history.pushState({}, '', url);
        } catch (err) {
            console.error('切换用户失败:', err);
            alert('加载用户数据失败');
            document.getElementById('loading').style.display = 'none';
        }
    }
    
    async loadUserData(secUid) {
        this.currentSecUid = secUid;
        const user = this.allUsers.find(u => u.sec_uid === secUid);
        
        this.setUserHeader(user);
        this.videoList = await (await fetch(`data/comment/${secUid}/video_list.json`)).json();
        this.baseUrl = this.videoList.base_url || '';
        this.videos = this.videoList.videos || [];
        this.filteredVideos = [...this.videos];
        this.commentsCache = {};
        
        this.generateYearOptions();
        this.updateStats();
        this.sortVideos();
        this.renderVideos(true);
        await this.loadSummary();
        
        localStorage.setItem('preferred_user', secUid);
    }
    
    generateYearOptions() {
        const years = new Set(this.videos.filter(v => v.create_time).map(v => new Date(v.create_time * 1000).getFullYear()));
        const box = document.getElementById('year-options');
        [...years].sort((a,b) => b-a).forEach(y => {
            const opt = document.createElement('div');
            opt.className = 'custom-select-option';
            opt.dataset.value = y;
            opt.textContent = `${y}年`;
            opt.addEventListener('click', () => {
                const w = document.getElementById('year-select-wrapper');
                w.querySelectorAll('.custom-select-option').forEach(o => o.classList.remove('active'));
                opt.classList.add('active');
                w.querySelector('.custom-select-trigger span').textContent = `${y}年`;
                w.querySelector('.custom-select-trigger').dataset.value = y;
                w.classList.remove('open');
                this.selectedYear = y;
                this.applyFilters();
            });
            box.appendChild(opt);
        });
    }
    
    async loadSummary() {
        try {
            const s = await (await fetch(`data/comment/${this.videoList.sec_uid}/summary.json`)).json();
            document.getElementById('generated-time').textContent = s.generated_at;
            if (s.active_repliers?.length) this.renderActiveRepliers([...s.active_repliers].sort(() => Math.random()-.5).slice(0,3));
        } catch {}
    }
    
    updateStats() {
        document.getElementById('total-videos').textContent = this.formatNumber(this.videoList.total_videos);
        document.getElementById('total-comments').textContent = this.formatNumber(this.videoList.total_comments);
    }
    
    renderActiveRepliers(repliers) {
        let c = document.getElementById('active-repliers');
        if (!c) { c = document.createElement('div'); c.id = 'active-repliers'; c.className = 'active-repliers'; document.getElementById('stats').appendChild(c); }
        const ph = this.getPlaceholderSvg('avatar');
        c.innerHTML = `<div class="repliers-label">活跃用户</div><div class="repliers-avatars">${
            repliers.map(r => `<div class="replier-item" tabindex="0" data-nickname="${this.escapeAttr(r.nickname)}" data-count="${r.count}">
                <img class="replier-avatar" src="${r.avatar ? this.getFullUrl(r.avatar) : ph}" alt="${this.escapeHtml(r.nickname)}" onerror="this.src='${ph}'">
                <div class="replier-popover"><div class="replier-popover-name">${this.escapeHtml(r.nickname)}</div><div class="replier-popover-count">${r.count} 条回复</div></div>
            </div>`).join('')
        }</div>`;
        c.querySelectorAll('.replier-popover-name').forEach(el => el.addEventListener('click', e => { e.stopPropagation(); this.searchByNickname(el.textContent); }));
        c.querySelectorAll('.replier-item').forEach(el => {
            el.addEventListener('mouseenter', () => this._adjustPopoverPosition(el));
            el.addEventListener('focus', () => this._adjustPopoverPosition(el));
        });
    }
    
    _adjustPopoverPosition(el) {
        const pop = el.querySelector('.replier-popover');
        if (!pop || el.dataset.adjusted) return;
        const r = pop.getBoundingClientRect();
        if (r.left < 10) {
            pop.style.left = '0'; pop.style.transform = 'translateX(0)'; pop.classList.add('arrow-left');
        } else if (r.right > window.innerWidth - 10) {
            pop.style.left = 'auto'; pop.style.right = '0'; pop.style.transform = 'none'; pop.classList.add('arrow-right');
        }
        el.dataset.adjusted = '1';
    }
    
    searchByNickname(n) {
        document.querySelector('#search-type-wrapper .custom-select-trigger').dataset.value = 'nickname';
        document.querySelector('#search-type-wrapper .custom-select-trigger span').textContent = '昵称';
        document.getElementById('search-input').value = n;
        this.search();
    }
    
    // ==================== 搜索 ====================
    
    async search() {
        const type = document.querySelector('#search-type-wrapper .custom-select-trigger').dataset.value;
        const q = document.getElementById('search-input').value.trim().toLowerCase();
        if (!q) { this.closeSearchResults(); return; }
        if (this.isSearching) return;
        this.isSearching = true;
        this.showSearchProgress(0, this.videos.length);
        const results = [];
        for (let i = 0; i < this.videos.length; i += 10) {
            const batch = this.videos.slice(i, i+10);
            const br = await Promise.all(batch.map(async v => {
                const comments = await this.loadComments(v.aweme_id);
                const vr = [];
                for (const c of comments) {
                    const mf = type === 'nickname' ? c.user_nickname : c.text;
                    if ((mf||'').toLowerCase().includes(q)) vr.push({ type:'comment', awemeId:v.aweme_id, videoTitle:v.desc||'', cid:c.cid, userNickname:c.user_nickname, text:c.text });
                    for (const r of c.replies || []) {
                        const rmf = type === 'nickname' ? r.user_nickname : r.text;
                        if ((rmf||'').toLowerCase().includes(q)) vr.push({ type:'reply', awemeId:v.aweme_id, videoTitle:v.desc||'', parentCid:c.cid, cid:r.cid, userNickname:r.user_nickname, text:r.text });
                    }
                }
                return vr;
            }));
            results.push(...br.flat());
            this.showSearchProgress(Math.min(i+10, this.videos.length), this.videos.length);
        }
        this.isSearching = false;
        this.searchResults = results;
        this.searchPage = 0;
        this.showSearchResults(q);
    }
    
    showSearchProgress(cur, total) {
        const c = document.getElementById('search-results');
        c.style.display = 'block';
        document.getElementById('search-count').textContent = '...';
        document.getElementById('search-results-list').innerHTML = `<div class="search-result-item" style="text-align:center;color:var(--text-secondary);padding:20px">正在搜索中... ${cur}/${total} 个作品</div>`;
        document.getElementById('search-pagination').innerHTML = '';
    }
    
    showSearchResults(query) {
        const c = document.getElementById('search-results'), list = document.getElementById('search-results-list');
        const total = this.searchResults.length, pages = Math.ceil(total / this.searchPageSize);
        document.getElementById('search-count').textContent = total;
        if (!total) { list.innerHTML = '<div class="search-result-item" style="text-align:center;color:var(--text-secondary)">没有找到匹配结果</div>'; document.getElementById('search-pagination').innerHTML = ''; }
        else {
            const start = this.searchPage * this.searchPageSize;
            list.innerHTML = this.searchResults.slice(start, start + this.searchPageSize).map(r => `
                <div class="search-result-item" data-aweme-id="${r.awemeId}" data-cid="${r.cid}" data-parent-cid="${r.parentCid||''}" data-type="${r.type}">
                    <div class="search-result-video"><span class="search-result-type ${r.type}">${r.type==='comment'?'评论':'回复'}</span><span class="search-result-video-title">${this.escapeHtml(r.videoTitle||'无标题')}</span></div>
                    <div class="search-result-content"><span class="search-result-user">${this.escapeHtml(r.userNickname||'匿名')} ：</span><span class="search-result-text">${this.highlightText(this.escapeHtml(r.text||''), query)}</span></div>
                </div>`).join('');
            list.querySelectorAll('.search-result-item').forEach(el => el.addEventListener('click', () => this.onSearchResultClick(el)));
            document.getElementById('search-pagination').innerHTML = pages > 1 ? this.createPaginationHtml(this.searchPage, pages, total, `goToSearchPage(\${page},'${this.escapeAttr(query)}')`) : '';
        }
        c.style.display = 'block';
    }
    
    goToSearchPage(p, q) { this.searchPage = p; this.showSearchResults(q); document.getElementById('search-results-list').scrollTop = 0; }
    highlightText(t, q) { return q ? t.replace(new RegExp(`(${q.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')})`,'gi'), '<span class="search-highlight">$1</span>') : t; }
    closeSearchResults() { document.getElementById('search-results').style.display = 'none'; }
    
    async onSearchResultClick(el) {
        const v = this.videos.find(v => v.aweme_id === el.dataset.awemeId);
        if (v) await this.openModal(v, el.dataset.cid, el.dataset.parentCid, el.dataset.type);
    }
    
    async loadComments(awemeId) {
        if (this.commentsCache[awemeId]) return this.commentsCache[awemeId];
        try {
            const d = await (await fetch(`data/comment/${this.videoList.sec_uid}/comments/${awemeId}.json`)).json();
            this.commentsCache[awemeId] = d.comments || [];
            return this.commentsCache[awemeId];
        } catch { return []; }
    }
    
    // ==================== 筛选/排序 ====================
    
    applyFilters() {
        this.filteredVideos = this.selectedYear
            ? this.videos.filter(v => new Date(v.create_time * 1000).getFullYear() === parseInt(this.selectedYear))
            : [...this.videos];
        this.sortVideos(); this.renderVideos(true); this.updateFilterInfo();
    }
    
    updateFilterInfo() {
        const el = document.getElementById('filter-info');
        if (this.selectedYear) { el.textContent = `筛选: ${this.selectedYear}年 | 共 ${this.filteredVideos.length} 个作品`; el.style.display = 'block'; }
        else el.style.display = 'none';
    }
    
    sortVideos() {
        const fn = { 'time-desc': (a,b) => (b.create_time||0)-(a.create_time||0), 'time-asc': (a,b) => (a.create_time||0)-(b.create_time||0), 'comments-desc': (a,b) => (b.comment_count||0)-(a.comment_count||0) };
        this.filteredVideos.sort(fn[this.currentSort] || fn['time-desc']);
    }
    
    // ==================== 视频列表 ====================
    
    renderVideos(reset = false) {
        const grid = document.getElementById('video-grid');
        if (reset) { grid.innerHTML = ''; this.currentPage = 0; }
        const start = this.currentPage * this.pageSize, end = start + this.pageSize;
        const batch = this.filteredVideos.slice(start, end);
        if (!batch.length && !this.currentPage) { grid.innerHTML = '<div class="no-results"><p>没有找到匹配的作品</p></div>'; document.getElementById('load-more').style.display = 'none'; return; }
        batch.forEach(v => grid.appendChild(this.createVideoCard(v)));
        this.currentPage++;
        document.getElementById('load-more').style.display = end < this.filteredVideos.length ? 'block' : 'none';
    }
    
    createVideoCard(video) {
        const card = document.createElement('div');
        card.className = 'video-card';
        card.onclick = () => this.openModal(video);
        const thumbUrl = this.getFullUrl(this.getMainUrls(video.thumb)[0] || this.getMainUrls(video.images)[0] || '');
        const imgCount = video.images?.length || 0;
        card.innerHTML = `
            <div class="video-thumbnail">
                ${thumbUrl ? `<img src="${thumbUrl}" alt="封面" loading="lazy" onerror="this.src='${this.getPlaceholderSvg('thumb')}'">` : '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#666">无图片</div>'}
                ${imgCount > 1 ? `<span class="image-count">🖼️ ${imgCount}</span>` : ''}
            </div>
            <div class="video-info">
                <div class="video-title">${this.escapeHtml(video.desc?.trim() || '无题')}</div>
                <div class="video-meta"><span class="video-date-time">${this.formatDateTime(video) || '未知时间'}</span><span class="video-comments">💬 ${video.comment_count || 0}</span></div>
            </div>`;
        return card;
    }
    
    // ==================== Modal ====================
    
    async openModal(video, highlightCid = null, parentCid = null, highlightType = null) {
        const modal = document.getElementById('video-modal'), body = document.getElementById('modal-body');
        let mediaHtml = '';
        const ph = this.getPlaceholderSvg('thumb');
        const hasImages = video.images?.length > 0;
        
        if (!hasImages && video.video?.length > 0) {
            mediaHtml = `<div class="video-player-wrapper"><video class="video-player" controls poster="${this.getFullUrl(this.getMainUrls(video.thumb)[0]||'')}" preload="metadata"><source src="${this.getFullUrl(this.getMainUrls(video.video)[0]||'')}" type="video/mp4">您的浏览器不支持视频播放</video></div>`;
        } else {
            const imgs = this.getMainUrls(video.images).map(u => this.getFullUrl(u));
            if (imgs.length > 1) {
                const uj = JSON.stringify(imgs);
                mediaHtml = `<div class="image-carousel"><div class="carousel-container" id="carousel-container">${
                    imgs.map((img,i) => `<img src="${img}" alt="图片${i+1}" loading="lazy" onerror="this.src='${ph}'" data-urls='${this.escapeAttr(uj)}' data-index="${i}" onclick="app.openSliderFromEl(this)">`).join('')
                }</div><button class="carousel-nav prev" onclick="app.carouselNav(-1)">‹</button><button class="carousel-nav next" onclick="app.carouselNav(1)">›</button></div><div class="carousel-dots">${
                    imgs.map((_,i) => `<button class="carousel-dot${i?'':' active'}" onclick="app.carouselGoTo(${i})"></button>`).join('')
                }</div>`;
                this.slider = { images: imgs, index: 0 };
            } else if (imgs.length === 1) {
                mediaHtml = `<div class="modal-images"><img src="${imgs[0]}" alt="图片1" loading="lazy" onerror="this.src='${ph}'" data-urls='${this.escapeAttr(JSON.stringify(imgs))}' data-index="0" onclick="app.openSliderFromEl(this)"></div>`;
            }
            if (hasImages && video.video?.length > 0) {
                mediaHtml += `<div class="bgm-player"><audio controls preload="none"><source src="${this.getFullUrl(this.getMainUrls(video.video)[0]||'')}" type="audio/mpeg"></audio></div>`;
            }
        }
        
        body.innerHTML = `${mediaHtml}<div class="modal-desc">${this.escapeHtml(video.desc||'无描述')}</div><div class="modal-meta"><span>📅 时间: ${this.formatDateTime(video)}</span><span>💬 评论数: ${video.comment_count||0}</span></div><div class="comments-section"><h3 class="comments-title">💬 评论加载中...</h3></div>`;
        modal.classList.add('active');
        const sbw = window.innerWidth - document.documentElement.clientWidth;
        document.body.style.overflow = 'hidden'; document.body.style.paddingRight = `${sbw}px`;
        
        this.currentComments = (await this.loadComments(video.aweme_id)) || [];
        this.commentsPage = 0; this.currentVideo = video;
        this.renderComments(highlightCid, parentCid, highlightType);
    }
    
    // ==================== 评论 ====================
    
    renderComments(hCid = null, pCid = null, hType = null) {
        const body = document.getElementById('modal-body');
        const total = this.currentComments.length, pages = Math.ceil(total / this.commentsPageSize);
        const hotN = Math.min(5, Math.ceil(total * 0.15));
        const sorted = this.getSortedComments(this.currentComments, hotN);
        
        if (hCid && !this.commentsPage) {
            for (let i = 0; i < sorted.length; i++) {
                if ((hType === 'comment' && sorted[i].cid === hCid) || (hType === 'reply' && pCid === sorted[i].cid)) {
                    this.commentsPage = Math.floor(i / this.commentsPageSize); break;
                }
            }
        }
        
        let html = '';
        if (total > 0) {
            const s = this.commentsPage * this.commentsPageSize, e = s + this.commentsPageSize;
            html = `<div class="comments-section"><h3 class="comments-title">💬 评论 (${total})</h3>${
                sorted.slice(s, e).map((c, i) => this.createCommentHtml(c, hCid, pCid, hType, s+i < hotN)).join('')
            }${pages > 1 ? `<div class="comments-pagination">${this.createPaginationHtml(this.commentsPage, pages, total, 'goToCommentsPage(${page})')}</div>` : ''}</div>`;
        } else {
            html = `<div class="comments-section"><h3 class="comments-title">💬 评论 (0)</h3><p style="color:var(--text-secondary);text-align:center;padding:20px">暂无评论</p></div>`;
        }
        
        body.querySelector('.comments-section')?.replaceWith(...new DOMParser().parseFromString(html, 'text/html').body.children);
        
        if (hCid) setTimeout(() => {
            const el = (hType === 'reply' && pCid) ? (document.getElementById(`reply-${hCid}`) || document.getElementById(`comment-${pCid}`)) : document.getElementById(`comment-${hCid}`);
            if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); el.style.background = '#fff3cd'; setTimeout(() => el.style.background = '', 3000); }
        }, 300);
    }
    
    getSortedComments(comments, hotN) {
        const byReply = [...comments].sort((a,b) => (b.reply_count||0) - (a.reply_count||0));
        const hot = byReply.slice(0, hotN), hotIds = new Set(hot.map(c => c.cid));
        const rest = comments.filter(c => !hotIds.has(c.cid)).sort((a,b) => (parseInt(b.create_time)||0) - (parseInt(a.create_time)||0));
        return [...hot, ...rest];
    }
    
    goToCommentsPage(p) { this.commentsPage = p; this.renderComments(); document.querySelector('.comments-section').scrollIntoView({ behavior: 'smooth' }); }
    
    createCommentHtml(comment, hCid = null, pCid = null, hType = null, isHot = false) {
        const replies = comment.replies?.length || 0;
        const isHL = hCid === comment.cid && hType === 'comment';
        const expand = hType === 'reply' && pCid === comment.cid;
        const ph = this.getPlaceholderSvg('avatar');
        
        let repliesHtml = '';
        if (comment.replies?.length) {
            const sorted = [...comment.replies].sort((a,b) => (parseInt(a.create_time)||0) - (parseInt(b.create_time)||0));
            repliesHtml = `<div class="replies-section">${sorted.map(r => {
                const rHL = hType === 'reply' && hCid === r.cid;
                return `<div class="reply-item" id="reply-${r.cid}" style="${rHL?'background:#fff3cd':''}"><div class="reply-header">${this.renderUserHeader(r,'reply',ph,r.reply_to_username)}</div><div class="reply-text">${this.renderContentHtml(r)}</div></div>`;
            }).join('')}</div>`;
        }
        
        return `<div class="comment-item${isHot?' hot-comment':''}" id="comment-${comment.cid}" style="${isHL?'background:#fff3cd':''}">
            <div class="comment-header">${this.renderUserHeader(comment,'comment',ph)}</div>
            <div class="comment-text">${this.renderContentHtml(comment)}</div>
            ${replies ? `<div class="replies-toggle-wrapper"><span class="replies-toggle" data-count="${replies}" onclick="app.toggleReplies(this)">展开 ${replies} 条回复</span></div><div class="replies-container" style="display:${expand?'block':'none'}">${repliesHtml}</div>` : ''}
        </div>`;
    }
    
    toggleReplies(t) {
        const c = t.parentElement.nextElementSibling;
        const count = t.dataset.count;
        const isHidden = c.style.display === 'none';
        c.style.display = isHidden ? 'block' : 'none';
        t.textContent = isHidden ? `收起回复` : `展开 ${count} 条回复`;
    }
    
    copyToClipboard(text, el) {
        navigator.clipboard.writeText(text).then(() => { const o = el.textContent; el.textContent = '已复制'; el.classList.add('copied'); setTimeout(() => { el.textContent = o; el.classList.remove('copied'); }, 1000); }).catch(() => {});
    }
    
    closeModal() {
        const modal = document.getElementById('video-modal');
        modal.querySelectorAll('video, audio').forEach(el => {
            el.pause();
            el.currentTime = 0;
        });
        modal.classList.remove('active');
        document.body.style.overflow = '';
        document.body.style.paddingRight = '';
    }
    loadMore() { this.renderVideos(); }
    
    async init() { await this.loadEmojiMap(); this.bindEvents(); await this.loadData(); }
}

const app = new App();
