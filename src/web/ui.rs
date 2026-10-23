//! Server-rendered administration pages.
//!
//! The panel deliberately remains a small client of `/api/admin/*`: it does
//! not embed database or acquisition logic, and it never renders passwords,
//! public feed bearer tokens, or upstream credentials. The styles are kept
//! inline so the administrator can use the panel in a minimal deployment
//! without an additional static-file server or frontend build step.

use axum::response::Html;

use crate::domain::{credentials::WeReadAccount, source::Source};

use super::{auth::AdminSession, i18n::Locale};

const STYLES: &str = r##"<style>
:root {
  color-scheme: light;
  --ink: #172033;
  --muted: #667085;
  --subtle: #98a2b3;
  --line: #e4e7ec;
  --surface: #ffffff;
  --surface-soft: #f8fafc;
  --canvas: #f3f6fb;
  --brand: #3855d8;
  --brand-dark: #2f45b6;
  --brand-soft: #eef1ff;
  --success: #087443;
  --success-soft: #e8f7ef;
  --warning: #b54708;
  --warning-soft: #fff5e8;
  --danger: #b42318;
  --danger-soft: #fff0ee;
  --shadow: 0 18px 45px rgba(16, 24, 40, .08);
  --radius-lg: 20px;
  --radius-md: 12px;
  --radius-sm: 8px;
}

* { box-sizing: border-box; }
html { min-width: 320px; scroll-behavior: smooth; }
body {
  min-height: 100vh;
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at 8% 0%, rgba(83, 111, 232, .12), transparent 34rem),
    var(--canvas);
  font: 15px/1.55 Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

a { color: var(--brand-dark); text-decoration: none; }
a:hover { text-decoration: underline; }
button, input, textarea { font: inherit; }
button, .button {
  display: inline-flex;
  min-height: 40px;
  align-items: center;
  justify-content: center;
  gap: 8px;
  border: 1px solid transparent;
  border-radius: var(--radius-sm);
  padding: 9px 14px;
  color: var(--ink);
  background: var(--surface);
  font-weight: 650;
  line-height: 1.2;
  cursor: pointer;
  transition: border-color .16s ease, background .16s ease, box-shadow .16s ease, transform .16s ease;
}
button:hover, .button:hover { text-decoration: none; transform: translateY(-1px); }
button:focus-visible, .button:focus-visible, a:focus-visible, input:focus-visible, textarea:focus-visible {
  outline: 3px solid rgba(56, 85, 216, .24);
  outline-offset: 2px;
}
button:disabled, .button[aria-disabled="true"] { cursor: not-allowed; opacity: .58; transform: none; }
.button-primary { color: #fff; background: var(--brand); box-shadow: 0 5px 14px rgba(56, 85, 216, .22); }
.button-primary:hover { background: var(--brand-dark); }
.button-secondary { border-color: #cdd5ff; color: var(--brand-dark); background: var(--brand-soft); }
.button-secondary:hover { border-color: #aebaff; background: #e5e9ff; }
.button-quiet { border-color: var(--line); background: var(--surface); }
.button-quiet:hover { border-color: #c5cad4; background: var(--surface-soft); }
.button-danger { border-color: #f1b8b2; color: var(--danger); background: var(--danger-soft); }
.button-danger:hover { border-color: #e9958c; background: #ffe5e1; }
.button-link { min-height: auto; border: 0; padding: 0; color: var(--brand-dark); background: transparent; }
.button-link:hover { transform: none; }

.site-header {
  border-bottom: 1px solid rgba(228, 231, 236, .84);
  background: rgba(255, 255, 255, .84);
  backdrop-filter: blur(16px);
}
.header-inner, .page-shell { width: min(1160px, calc(100% - 40px)); margin: 0 auto; }
.header-inner { min-height: 72px; display: flex; align-items: center; justify-content: space-between; gap: 20px; }
.brand { display: inline-flex; align-items: center; gap: 11px; color: var(--ink); }
.brand:hover { text-decoration: none; }
.brand-mark {
  display: grid;
  width: 37px;
  height: 37px;
  place-items: center;
  border-radius: 11px;
  color: #fff;
  background: linear-gradient(135deg, #6078ed, #324dcc);
  box-shadow: 0 7px 14px rgba(56, 85, 216, .24);
  font-size: 18px;
  font-weight: 800;
}
.brand-copy { display: grid; gap: 0; font-weight: 780; letter-spacing: -.02em; }
.brand-copy small { color: var(--muted); font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.header-actions { display: flex; align-items: center; gap: 14px; }
.identity { display: inline-flex; align-items: center; gap: 7px; color: var(--muted); font-size: 13px; }
.identity::before { width: 7px; height: 7px; border-radius: 50%; background: #12b76a; content: ""; }
.locale-picker { display: inline-flex; align-items: center; gap: 6px; color: var(--muted); font-size: 12px; font-weight: 650; }
.locale-picker select { min-height: 34px; border: 1px solid var(--line); border-radius: var(--radius-sm); padding: 5px 25px 5px 8px; color: var(--ink); background: var(--surface); font: inherit; cursor: pointer; }
.locale-picker select:hover { border-color: #c5cad4; }
.auth-toolbar { display: flex; justify-content: flex-end; margin-bottom: 12px; }
.sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; }

.page-shell { padding: 42px 0 72px; }
.page-header { display: flex; align-items: flex-end; justify-content: space-between; gap: 28px; margin-bottom: 30px; }
.page-header h1 { margin: 5px 0 8px; color: var(--ink); font-size: clamp(28px, 4vw, 38px); letter-spacing: -.045em; line-height: 1.1; }
.page-header p { max-width: 680px; margin: 0; color: var(--muted); }
.page-header-actions, .form-actions, .resource-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 9px; }
.page-header-actions { justify-content: flex-end; }
.kicker { margin: 0; color: var(--brand); font-size: 11px; font-weight: 800; letter-spacing: .13em; text-transform: uppercase; }
.eyebrow { color: var(--muted); font-size: 13px; }
.muted { color: var(--muted); }
.small { color: var(--muted); font-size: 12px; }

.card {
  border: 1px solid rgba(228, 231, 236, .95);
  border-radius: var(--radius-lg);
  padding: 24px;
  background: var(--surface);
  box-shadow: var(--shadow);
}
.card-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 18px; margin-bottom: 20px; }
.card-header h2, .card-header h3 { margin: 0 0 5px; color: var(--ink); font-size: 18px; letter-spacing: -.02em; }
.card-header p { margin: 0; color: var(--muted); font-size: 13px; }
.card-icon { display: grid; width: 38px; height: 38px; flex: 0 0 auto; place-items: center; border-radius: 11px; color: var(--brand-dark); background: var(--brand-soft); font-weight: 800; }
.card-icon.green { color: var(--success); background: var(--success-soft); }
.card-icon.orange { color: var(--warning); background: var(--warning-soft); }
.card-icon.red { color: var(--danger); background: var(--danger-soft); }
.layout-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; }
.full-width { grid-column: 1 / -1; }
.stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-bottom: 20px; }
.stat { display: flex; align-items: center; gap: 14px; border: 1px solid var(--line); border-radius: var(--radius-md); padding: 17px; background: rgba(255, 255, 255, .7); }
.stat-value { display: block; color: var(--ink); font-size: 25px; font-weight: 780; letter-spacing: -.04em; line-height: 1.1; }
.stat-label { display: block; margin-top: 4px; color: var(--muted); font-size: 12px; }
.stat-icon { display: grid; width: 35px; height: 35px; flex: 0 0 auto; place-items: center; border-radius: 10px; color: var(--brand); background: var(--brand-soft); font-size: 13px; font-weight: 800; }
.stat-icon.green { color: var(--success); background: var(--success-soft); }
.stat-icon.orange { color: var(--warning); background: var(--warning-soft); }

.notice { display: flex; gap: 11px; border-radius: var(--radius-md); margin: 0 0 20px; padding: 13px 14px; color: var(--muted); background: var(--surface-soft); font-size: 13px; }
.notice strong { color: var(--ink); }
.notice.info { border: 1px solid #d8defe; color: #4352a4; background: var(--brand-soft); }
.notice.warning { border: 1px solid #f9d8a9; color: #8d480d; background: var(--warning-soft); }
.notice-icon { font-weight: 800; }
.feedback { border-radius: var(--radius-sm); margin: 14px 0 0; padding: 10px 12px; font-size: 13px; }
.feedback[hidden] { display: none; }
.feedback.error { color: var(--danger); background: var(--danger-soft); }
.feedback.success { color: var(--success); background: var(--success-soft); }
.feedback.info { color: #4352a4; background: var(--brand-soft); }
#qr-image { display: grid; width: min(100%, 260px); min-height: 220px; place-items: center; border: 1px solid var(--line); border-radius: var(--radius-md); margin: 6px auto 15px; padding: 10px; background: #fff; }
#qr-image:empty { display: none; }
#qr-image img { display: block; width: 100%; height: auto; }

form { display: grid; gap: 17px; }
.field-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 17px 15px; }
.field-grid .wide { grid-column: 1 / -1; }
.field-grid .wide > .small { display: block; margin-top: -3px; }
label { display: grid; gap: 7px; color: var(--ink); font-size: 13px; font-weight: 650; }
.label-row { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
.label-hint { color: var(--subtle); font-size: 11px; font-weight: 500; }
input, textarea {
  width: 100%;
  border: 1px solid #d0d5dd;
  border-radius: var(--radius-sm);
  padding: 10px 11px;
  color: var(--ink);
  background: #fff;
  transition: border-color .16s ease, box-shadow .16s ease;
}
input { min-height: 42px; }
input[type="checkbox"] { width: auto; min-height: auto; }
textarea { min-height: 106px; resize: vertical; }
input::placeholder, textarea::placeholder { color: #98a2b3; }
input:hover, textarea:hover { border-color: #98a2b3; }
input:focus, textarea:focus { border-color: var(--brand); box-shadow: 0 0 0 3px rgba(56, 85, 216, .12); outline: 0; }
fieldset { min-width: 0; border: 0; margin: 0; padding: 0; }
legend { margin-bottom: 15px; color: var(--ink); font-size: 14px; font-weight: 760; }
.form-section + .form-section { border-top: 1px solid var(--line); padding-top: 21px; }
.form-actions { border-top: 1px solid var(--line); padding-top: 18px; }
.form-actions .button-primary { margin-left: auto; }

.resource-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 13px; }
.resource-card { border: 1px solid var(--line); border-radius: var(--radius-md); padding: 16px; background: var(--surface); }
.resource-card:hover { border-color: #c6cff8; box-shadow: 0 7px 20px rgba(16, 24, 40, .06); }
.resource-card-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
.resource-card h3 { overflow: hidden; margin: 0; color: var(--ink); font-size: 15px; text-overflow: ellipsis; white-space: nowrap; }
.resource-card p { margin: 0; }
.resource-meta { display: grid; gap: 5px; margin: 0 0 15px; }
.resource-meta-row { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 12px; }
.resource-meta-row strong { overflow: hidden; color: var(--ink); font-size: 12px; font-weight: 600; text-align: right; text-overflow: ellipsis; white-space: nowrap; }
.resource-actions { border-top: 1px solid var(--line); padding-top: 13px; }
.resource-actions .button { min-height: 35px; padding: 7px 10px; font-size: 12px; }
.resource-actions .button-link { padding: 7px 2px; }
.resource-actions .spacer { flex: 1; }
.feed-result { overflow-wrap: anywhere; margin-top: 11px !important; font-size: 12px; }
.feed-result a { font-weight: 650; }
details.history { border-top: 1px solid var(--line); margin-top: 13px; padding-top: 11px; color: var(--muted); font-size: 12px; }
details.history summary { color: var(--brand-dark); cursor: pointer; font-weight: 650; }
details.history ul { display: grid; gap: 4px; margin: 9px 0 0; padding-left: 18px; }
.status-chip { display: inline-flex; flex: 0 0 auto; align-items: center; gap: 6px; border-radius: 999px; padding: 4px 9px; color: var(--muted); background: var(--surface-soft); font-size: 11px; font-weight: 750; letter-spacing: .02em; text-transform: capitalize; }
.status-chip::before { width: 6px; height: 6px; border-radius: 50%; background: var(--subtle); content: ""; }
.status-chip.enabled, .status-chip.active, .status-chip.ready { color: var(--success); background: var(--success-soft); }
.status-chip.enabled::before, .status-chip.active::before, .status-chip.ready::before { background: #12b76a; }
.status-chip.paused, .status-chip.disabled { color: var(--muted); background: #f2f4f7; }
.status-chip.warning, .status-chip.expired, .status-chip.blocked { color: var(--warning); background: var(--warning-soft); }
.status-chip.warning::before, .status-chip.expired::before, .status-chip.blocked::before { background: #f79009; }
.status-chip.error { color: var(--danger); background: var(--danger-soft); }
.status-chip.error::before { background: #f04438; }
.loading-state, .empty-state, .error-state { grid-column: 1 / -1; border: 1px dashed #cdd5df; border-radius: var(--radius-md); padding: 28px 18px; color: var(--muted); text-align: center; background: var(--surface-soft); }
.empty-state strong { display: block; margin-bottom: 4px; color: var(--ink); }
.error-state { border-color: #f1b8b2; color: var(--danger); background: var(--danger-soft); }
code { border-radius: 5px; padding: 2px 5px; color: #344054; background: #f2f4f7; font-size: .9em; }
.definition-list { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 13px; margin: 0; }
.definition-list div { border: 1px solid var(--line); border-radius: var(--radius-sm); padding: 12px; background: var(--surface-soft); }
.definition-list dt { color: var(--muted); font-size: 11px; font-weight: 700; letter-spacing: .04em; text-transform: uppercase; }
.definition-list dd { margin: 4px 0 0; color: var(--ink); font-weight: 700; }
.danger-zone { border-color: #f4d1cd; box-shadow: none; }
.danger-zone .card-header h2 { color: var(--danger); }
.breadcrumb { display: flex; flex-wrap: wrap; align-items: center; gap: 7px; margin-bottom: 23px; color: var(--muted); font-size: 13px; }
.breadcrumb span { color: var(--subtle); }
.split-panel { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(260px, .65fr); gap: 20px; align-items: start; }
.side-stack { display: grid; gap: 20px; }

.auth-page { display: grid; min-height: 100vh; place-items: center; padding: 28px 20px; }
.auth-layout { display: grid; grid-template-columns: minmax(0, 420px) minmax(230px, 290px); gap: 22px; align-items: stretch; width: min(760px, 100%); }
.auth-card { border: 1px solid rgba(228, 231, 236, .95); border-radius: 24px; padding: clamp(25px, 6vw, 42px); background: rgba(255, 255, 255, .94); box-shadow: var(--shadow); }
.auth-card .brand { margin-bottom: 40px; }
.auth-intro h1 { margin: 7px 0 9px; color: var(--ink); font-size: 30px; letter-spacing: -.045em; line-height: 1.1; }
.auth-intro p { margin: 0 0 27px; color: var(--muted); }
.auth-card .form-actions { margin-top: 5px; }
.auth-card .form-actions .button-primary { width: 100%; margin: 0; }
.auth-note { display: flex; flex-direction: column; justify-content: flex-end; border-radius: var(--radius-lg); padding: 23px; color: #dbe3ff; background: linear-gradient(155deg, #263b9f, #18265f); box-shadow: var(--shadow); }
.auth-note strong { display: block; margin-bottom: 6px; color: #fff; font-size: 16px; }
.auth-note p { margin: 0; color: #bfcaff; font-size: 13px; }
.auth-note ul { display: grid; gap: 10px; margin: 18px 0 0; padding: 0; list-style: none; color: #dbe3ff; font-size: 13px; }
.auth-note li { display: flex; gap: 8px; }
.auth-note li::before { color: #9eafff; content: "✓"; font-weight: 800; }

@media (max-width: 840px) {
  .layout-grid, .split-panel, .auth-layout { grid-template-columns: 1fr; }
  .auth-note { min-height: 170px; }
}
@media (max-width: 620px) {
  .header-inner, .page-shell { width: min(100% - 28px, 1160px); }
  .header-inner { min-height: 62px; }
  .identity { display: none; }
  .page-shell { padding: 27px 0 48px; }
  .page-header { display: grid; align-items: start; gap: 16px; margin-bottom: 22px; }
  .page-header-actions { justify-content: flex-start; }
  .stats, .resource-grid, .field-grid, .definition-list { grid-template-columns: 1fr; }
  .full-width, .field-grid .wide { grid-column: auto; }
  .card { padding: 18px; border-radius: 16px; }
  .form-actions .button-primary { width: 100%; margin-left: 0; }
  .auth-page { padding: 14px; }
  .auth-card { padding: 25px 20px; }
  .auth-card .brand { margin-bottom: 32px; }
  .auth-note { display: none; }
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { scroll-behavior: auto !important; transition-duration: .01ms !important; animation-duration: .01ms !important; }
}
</style>"##;

const LOCALE_PICKER: &str = r##"<label class="locale-picker"><span class="sr-only" data-i18n="language.label">Language</span><select id="locale" data-i18n-aria-label="language.label" aria-label="Language"><option value="en" data-i18n="language.english">English</option><option value="fr" data-i18n="language.french">Français</option><option value="zh" data-i18n="language.chinese">中文</option></select></label>"##;

const CONFIG_TRANSFER_CARD: &str = r##"<section class="card full-width" id="configuration" aria-labelledby="config-heading"><div class="card-header"><div><h2 id="config-heading" data-i18n="config.heading">Configuration backup</h2><p data-i18n="config.description">Export or preview configuration before applying it.</p></div><span class="card-icon" aria-hidden="true">⇅</span></div><fieldset><legend data-i18n="config.export_sections">Export sections</legend><div class="field-grid"><label for="config-export-sources"><span data-i18n="config.sources">Sources</span><input id="config-export-sources" name="sources" type="checkbox" checked></label><label for="config-export-accounts"><span data-i18n="config.accounts">WeRead account settings</span><input id="config-export-accounts" name="weread_accounts" type="checkbox" checked></label></div><div class="form-actions"><button id="config-export" class="button button-secondary" type="button" data-i18n="config.export">Download JSON</button></div></fieldset><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span data-i18n="config.credential_notice">Account settings are exported without credentials. Imported accounts need credential setup before use.</span></div><div class="form-section"><h3 data-i18n="config.import_heading">Import configuration</h3><label for="config-file"><span data-i18n="config.choose_file">Choose a configuration JSON file</span><input id="config-file" type="file" accept="application/json,.json"></label><div class="form-actions"><button id="config-preview" class="button button-secondary" type="button" data-i18n="config.preview">Preview import</button><button id="config-apply" class="button button-primary" type="button" disabled data-i18n="config.apply">Apply import</button></div><p id="config-feedback" class="feedback" role="status" aria-live="polite" hidden></p><section id="config-preview-result" aria-live="polite" hidden><h3 id="config-result-heading" data-i18n="config.result_heading">Import result</h3><div id="config-counts" class="resource-grid"></div><div id="config-conflicts" hidden><h4 data-i18n="config.conflicts_heading">Conflicts</h4><ul id="config-conflict-list"></ul></div></section></div><div class="notice warning"><span class="notice-icon" aria-hidden="true">!</span><span data-i18n="config.unsupported">Articles, synchronization history, and cached assets are not included.</span></div></section>"##;

const CONFIG_TRANSFER_SCRIPT: &str = r##"
const configSources=document.querySelector('#config-export-sources');const configAccounts=document.querySelector('#config-export-accounts');const configExport=document.querySelector('#config-export');const configFile=document.querySelector('#config-file');const configPreview=document.querySelector('#config-preview');const configApply=document.querySelector('#config-apply');const configFeedback=document.querySelector('#config-feedback');const configResult=document.querySelector('#config-preview-result');const configCounts=document.querySelector('#config-counts');const configConflicts=document.querySelector('#config-conflicts');const configConflictList=document.querySelector('#config-conflict-list');let configBundle=null;let configPreviewValid=false;let configPreviewRequestId=0;
function resetConfigPreview(){configPreviewRequestId++;configBundle=null;configPreviewValid=false;configApply.disabled=true;configResult.hidden=true;configCounts.replaceChildren();configConflictList.replaceChildren();configConflicts.hidden=true;feedback(configFeedback,'')}
function renderConfigCounts(value){const fields=[['config.sources_created','sources_created'],['config.sources_updated','sources_updated'],['config.accounts_created','accounts_created'],['config.accounts_updated','accounts_updated'],['config.placeholders_created','placeholders_created'],['config.bindings_preserved','account_bindings_preserved'],['config.bindings_cleared','account_bindings_cleared']];configCounts.replaceChildren(...fields.map(([key,name])=>meta(t(key),String(value[name]||0))))}
configSources.addEventListener('change',()=>{configExport.disabled=!(configSources.checked||configAccounts.checked)});configAccounts.addEventListener('change',()=>{configExport.disabled=!(configSources.checked||configAccounts.checked)});
configExport.addEventListener('click',async()=>{if(!configSources.checked&&!configAccounts.checked){feedback(configFeedback,t('config.export_selection_required'));return}configExport.disabled=true;feedback(configFeedback,'');const query=new URLSearchParams({sources:String(configSources.checked),weread_accounts:String(configAccounts.checked)});try{const response=await request('/api/admin/config/export?'+query);if(!response.ok){feedback(configFeedback,await apiErrorMessage(response,'config.export_failed'));return}const blob=await response.blob();const url=URL.createObjectURL(blob);const link=document.createElement('a');link.href=url;link.download='werrss-config.json';document.body.append(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);feedback(configFeedback,'','success')}catch{feedback(configFeedback,t('common.unreachable'))}finally{configExport.disabled=!(configSources.checked||configAccounts.checked)}});
configFile.addEventListener('change',()=>{resetConfigPreview();configPreview.disabled=false});
configPreview.addEventListener('click',async()=>{resetConfigPreview();const file=configFile.files&&configFile.files[0];if(!file){feedback(configFeedback,t('config.no_file'));return}if(file.size>2097152){feedback(configFeedback,t('config.file_too_large'));return}const configPreviewRequest=++configPreviewRequestId;configPreview.disabled=true;try{configBundle=await file.text();if(configPreviewRequest!==configPreviewRequestId){return}const response=await request('/api/admin/config/import/preview',{method:'POST',body:configBundle});if(configPreviewRequest!==configPreviewRequestId){return}if(!response.ok){const message=await apiErrorMessage(response,'config.preview_failed');if(configPreviewRequest!==configPreviewRequestId){return}configBundle=null;feedback(configFeedback,message);return}const value=await response.json();if(configPreviewRequest!==configPreviewRequestId){return}renderConfigCounts(value);const conflicts=Array.isArray(value.conflicts)?value.conflicts:[];configConflictList.replaceChildren(...conflicts.map(conflict=>{const item=document.createElement('li');item.textContent=conflict.message;return item}));configConflicts.hidden=conflicts.length===0;configPreviewValid=conflicts.length===0;configApply.disabled=!configPreviewValid;configResult.hidden=false;feedback(configFeedback,t(configPreviewValid?'config.preview_ready':'config.preview_conflicts'),configPreviewValid?'success':'info')}catch{if(configPreviewRequest===configPreviewRequestId){configBundle=null;feedback(configFeedback,t('common.unreachable'))}}finally{if(configPreviewRequest===configPreviewRequestId){configPreview.disabled=false}}});
configApply.addEventListener('click',async()=>{if(!configPreviewValid||!configBundle){return}configApply.disabled=true;try{const response=await request('/api/admin/config/import',{method:'POST',body:configBundle});if(!response.ok){feedback(configFeedback,await apiErrorMessage(response,'config.apply_failed'));configApply.disabled=false;return}const value=await response.json();renderConfigCounts(value);configResult.hidden=false;configPreviewValid=false;configBundle=null;feedback(configFeedback,t('config.applied'),'success')}catch{feedback(configFeedback,t('common.unreachable'));configApply.disabled=false}});
"##;

const QR_LOGIN_CARD: &str = r##"<section class="card" id="weread-qr-card"><div class="card-header"><div><h2 data-i18n="account.qr_heading">Sign in with QR code</h2><p data-i18n="account.qr_description">Scan a fresh WeRead code to enroll an account without copying cookies.</p></div><span class="card-icon" aria-hidden="true">▣</span></div><div class="notice"><span class="notice-icon" aria-hidden="true">i</span><span data-i18n="account.qr_notice">The QR code is short-lived and shown only while this page is open.</span></div><form id="weread-qr"><label><span class="label-row"><span data-i18n="account.id">Account ID</span><span class="label-hint" data-i18n="account.id_hint_qr">Optional; leave empty for a new account</span></span><input name="account_id" type="text" autocomplete="off" data-i18n-placeholder="account.id_placeholder_qr" placeholder="Optional existing account UUID"></label><label><span data-i18n="account.display_name">Display name</span><input name="display_name" type="text" autocomplete="off" data-i18n-placeholder="account.display_name_placeholder_qr" placeholder="Optional override"></label><div class="form-actions"><button class="button-secondary" type="submit" data-i18n="account.start_qr">Start QR login</button></div></form><div id="qr-session" hidden><div id="qr-image" aria-live="polite"></div><p id="qr-status" class="feedback info" role="status"></p><button id="qr-cancel" class="button-quiet" type="button" data-i18n="account.cancel_qr">Cancel QR login</button></div><p id="qr-result" class="feedback" role="status" hidden></p></section>"##;

const QR_LOGIN_SCRIPT: &str = r##"
const qrForm=document.querySelector('#weread-qr');const qrSession=document.querySelector('#qr-session');const qrImage=document.querySelector('#qr-image');const qrStatus=document.querySelector('#qr-status');const qrResult=document.querySelector('#qr-result');const qrCancel=document.querySelector('#qr-cancel');let qrAttemptId=null;let qrTimer=null;let qrPollGeneration=0;let qrStartInFlight=false;
function qrMessage(status){return t('account.qr_'+status)}
function stopQrTimer(){if(qrTimer){clearTimeout(qrTimer);qrTimer=null}}
function finishQrAttempt(){qrAttemptId=null;qrPollGeneration++;stopQrTimer();qrForm.querySelector('button[type="submit"]').disabled=false}
function showQrResult(message,kind='error'){qrResult.textContent=message;qrResult.className='feedback '+kind;qrResult.hidden=!message}
async function pollQr(attemptId,generation){if(!attemptId||generation!==qrPollGeneration||attemptId!==qrAttemptId){return}try{const response=await request('/api/admin/weread/qr/'+encodeURIComponent(attemptId));if(generation!==qrPollGeneration||attemptId!==qrAttemptId){return}if(!response.ok){const message=await apiErrorMessage(response,'account.qr_poll_failed');showQrResult(message);stopQrTimer();return}const value=await response.json();if(generation!==qrPollGeneration||attemptId!==qrAttemptId){return}if(value.status==='completed'){showQrResult(t('common.account_saved_detail').replace('{id}',value.account.account_id),'success');qrSession.hidden=true;finishQrAttempt();await loadAccounts();return}if(value.status==='expired'||value.status==='risk_controlled'){showQrResult(qrMessage(value.status));qrSession.hidden=true;finishQrAttempt();return}qrStatus.textContent=qrMessage(value.status);qrTimer=setTimeout(()=>pollQr(attemptId,generation),2000)}catch{if(generation===qrPollGeneration&&attemptId===qrAttemptId){showQrResult(t('common.unreachable'));stopQrTimer()}}}
qrForm.addEventListener('submit',async event=>{event.preventDefault();if(qrAttemptId||qrStartInFlight){return}const submit=qrForm.querySelector('button[type="submit"]');qrStartInFlight=true;submit.disabled=true;showQrResult('');const form=new FormData(qrForm);const accountId=form.get('account_id');try{const response=await request('/api/admin/weread/qr',{method:'POST',body:JSON.stringify({account_id:accountId&&accountId.trim()?accountId.trim():null,display_name:form.get('display_name')&&form.get('display_name').trim()?form.get('display_name').trim():null})});if(!response.ok){showQrResult(await apiErrorMessage(response,'account.qr_start_failed'));return}const value=await response.json();qrAttemptId=value.attempt_id;const generation=qrPollGeneration;qrImage.replaceChildren();const image=document.createElement('img');image.alt=t('account.qr_image_alt');image.src='data:image/svg+xml;charset=utf-8,'+encodeURIComponent(value.qr_svg);qrImage.append(image);qrStatus.textContent=t('account.qr_waiting_for_scan');qrSession.hidden=false;stopQrTimer();pollQr(value.attempt_id,generation)}catch{showQrResult(t('common.unreachable'))}finally{qrStartInFlight=false;if(!qrAttemptId){submit.disabled=false}}});
qrCancel.addEventListener('click',async()=>{const attemptId=qrAttemptId;const generation=qrPollGeneration;if(!attemptId){return}qrCancel.disabled=true;try{const response=await request('/api/admin/weread/qr/'+encodeURIComponent(attemptId),{method:'DELETE'});if(generation!==qrPollGeneration||attemptId!==qrAttemptId){return}if(response.ok){showQrResult(t('account.qr_cancelled'),'info');qrSession.hidden=true;finishQrAttempt()}else{showQrResult(await apiErrorMessage(response,'account.qr_cancel_failed'))}}catch{if(generation===qrPollGeneration&&attemptId===qrAttemptId){showQrResult(t('common.unreachable'))}}finally{qrCancel.disabled=false}});
"##;

fn i18n_bootstrap(locale: Locale) -> String {
    let translations = super::i18n::translations_json(locale)
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    format!(
        "const translations={translations};const t=key=>Object.prototype.hasOwnProperty.call(translations,key)?translations[key]:key;const statusLabel=status=>Object.prototype.hasOwnProperty.call(translations,`status.${{status}}`)?t(`status.${{status}}`):status;document.documentElement.lang='{locale}';const translateStatic=()=>{{document.querySelectorAll('[data-i18n]').forEach(element=>{{element.textContent=t(element.dataset.i18n)}});document.querySelectorAll('[data-i18n-placeholder]').forEach(element=>{{element.placeholder=t(element.dataset.i18nPlaceholder)}});document.querySelectorAll('[data-i18n-aria-label]').forEach(element=>{{element.setAttribute('aria-label',t(element.dataset.i18nAriaLabel))}})}};const addLocalePicker=()=>{{let localeSelect=document.querySelector('#locale');if(!localeSelect){{const host=document.querySelector('.header-actions')||document.querySelector('.auth-toolbar');if(!host)return;const label=document.createElement('label');label.className='locale-picker';const hidden=document.createElement('span');hidden.className='sr-only';hidden.dataset.i18n='language.label';hidden.textContent='Language';localeSelect=document.createElement('select');localeSelect.id='locale';localeSelect.dataset.i18nAriaLabel='language.label';localeSelect.setAttribute('aria-label','Language');[['en','language.english','English'],['fr','language.french','Français'],['zh','language.chinese','中文']].forEach(([value,key,labelText])=>{{const option=document.createElement('option');option.value=value;option.dataset.i18n=key;option.textContent=labelText;localeSelect.append(option)}});label.append(hidden,localeSelect);host.append(label)}}localeSelect.value='{locale}';localeSelect.addEventListener('change',()=>{{document.cookie=`werrss_locale=${{encodeURIComponent(localeSelect.value)}}; Path=/; Max-Age=31536000; SameSite=Lax`;location.reload()}})}};translateStatic();addLocalePicker();translateStatic();",
        translations = translations,
        locale = locale.code(),
    )
}

/// Renders the public login page. Credentials are submitted to the JSON API.
pub fn login_page(locale: Locale) -> Html<String> {
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title data-i18n="login.title">Sign in — Werrss admin</title>__STYLES__</head>
<body class="auth-page"><main class="auth-layout"><section class="auth-card" aria-labelledby="login-title"><div class="auth-toolbar">__LOCALE_PICKER__</div><a class="brand" href="/admin/login"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small data-i18n="app.admin_console">Admin console</small></span></a><div class="auth-intro"><p class="kicker" data-i18n="login.kicker">Private reader</p><h1 id="login-title" data-i18n="login.heading">Welcome back</h1><p data-i18n="login.description">Sign in to manage your sources, credentials, and feed links.</p></div><form id="login"><label><span data-i18n="login.username">Username</span><input name="username" autocomplete="username" required autofocus></label><label><span data-i18n="login.password">Password</span><input name="password" type="password" autocomplete="current-password" required></label><div class="form-actions"><button class="button-primary" type="submit"><span data-i18n="login.sign_in">Sign in</span></button></div><p id="error" class="feedback error" role="alert" hidden></p></form></section><aside class="auth-note" data-i18n-aria-label="login.about_aria" aria-label="About the admin console"><strong data-i18n="login.note_heading">A quiet place to manage your feeds.</strong><p data-i18n="login.note_description">Everything here is designed for one trusted administrator.</p><ul><li data-i18n="login.feature_controls">Protected source and account controls</li><li data-i18n="login.feature_links">Copyable RSS feed links</li><li data-i18n="login.feature_no_secrets">No secrets displayed after saving</li></ul></aside></main>
<script>
__I18N__
const loginForm=document.querySelector('#login');const loginError=document.querySelector('#error');const loginButton=loginForm.querySelector('button');
loginForm.addEventListener('submit',async event=>{event.preventDefault();loginError.hidden=true;loginButton.disabled=true;loginButton.setAttribute('aria-busy','true');const form=new FormData(event.target);try{const response=await fetch('/api/admin/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({username:form.get('username'),password:form.get('password')})});if(response.ok){location='/admin/';return}loginError.textContent=t('login.error.invalid');loginError.hidden=false}catch{loginError.textContent=t('login.error.unreachable');loginError.hidden=false}finally{loginButton.disabled=false;loginButton.removeAttribute('aria-busy')}});
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__LOCALE_PICKER__", LOCALE_PICKER)
            .replace("__I18N__", &i18n_bootstrap(locale)),
    )
}

/// Renders the authenticated source-management panel.
pub fn admin_page(session: &AdminSession, locale: Locale) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title data-i18n="dashboard.title">Dashboard — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small data-i18n="app.admin_console">Admin console</small></span></a><div class="header-actions"><span class="identity"><span data-i18n="nav.signed_in_as">Signed in as</span> <strong>__USERNAME__</strong></span><button id="logout" class="button button-quiet" type="button" data-i18n="nav.sign_out">Sign out</button>__LOCALE_PICKER__</div></div></header><main class="page-shell"><section class="page-header"><div><p class="kicker" data-i18n="dashboard.kicker">Workspace overview</p><h1 data-i18n="dashboard.heading">Good to see you.</h1><p data-i18n="dashboard.description">Keep your feeds healthy, credentials current, and delivery links close at hand.</p></div><div class="page-header-actions"><a class="button button-secondary" href="#create" data-i18n="dashboard.add_source">Add source</a><a class="button button-quiet" href="/admin/weread/accounts" data-i18n="dashboard.manage_accounts">Manage accounts</a></div></section><section class="stats" data-i18n-aria-label="dashboard.summary_aria" aria-label="Workspace summary"><div class="stat"><span class="stat-icon">S</span><span><strong id="source-count" class="stat-value">—</strong><span class="stat-label" data-i18n="stats.sources">Sources</span></span></div><div class="stat"><span class="stat-icon green">A</span><span><strong id="active-source-count" class="stat-value">—</strong><span class="stat-label" data-i18n="stats.active_sources">Active sources</span></span></div><div class="stat"><span class="stat-icon orange">W</span><span><strong id="account-count" class="stat-value">—</strong><span class="stat-label" data-i18n="stats.weread_accounts">WeRead accounts</span></span></div></section><div class="layout-grid"><section class="card" id="weread-account-card"><div class="card-header"><div><h2 data-i18n="account.connect_heading">Connect WeRead</h2><p data-i18n="account.connect_description">Enroll a browser session for authenticated source sync.</p></div><span class="card-icon green" aria-hidden="true">W</span></div><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span data-i18n="account.cookies_notice">Cookies are encrypted before storage and are never shown again. You can replace them later from the account page.</span></div><form id="weread-account"><label><span class="label-row"><span data-i18n="account.id">Account ID</span><span class="label-hint" data-i18n="account.id_hint_new">Optional for a new account</span></span><input name="account_id" type="text" autocomplete="off" data-i18n-placeholder="account.id_placeholder" placeholder="Leave blank to create an ID"></label><label><span class="label-row"><span data-i18n="account.display_name">Display name</span><span class="label-hint" data-i18n="account.display_name_hint">Optional when wr_name is present</span></span><input name="display_name" type="text" autocomplete="off" data-i18n-placeholder="account.display_name_placeholder" placeholder="e.g. Personal account"></label><label><span data-i18n="account.cookie_header">WeRead Cookie header</span><textarea name="cookie_header" rows="4" required autocomplete="off" data-i18n-placeholder="account.cookie_placeholder" placeholder="wr_vid=…; wr_skey=…; wr_rt=…"></textarea></label><label><span data-i18n="account.access_expiry">Access token expiry</span><input name="access_expires_at" type="datetime-local" required></label><div class="form-actions"><button class="button-primary" type="submit" data-i18n="account.save">Save account</button></div></form><p id="account-result" class="feedback" role="status" hidden></p></section><section class="card" id="create"><div class="card-header"><div><h2 data-i18n="source.add_heading">Add a source</h2><p data-i18n="source.add_description">Start with a Book ID or resolve one from an article URL.</p></div><span class="card-icon" aria-hidden="true">+</span></div><div class="notice"><span class="notice-icon" aria-hidden="true">↗</span><span data-i18n="source.unbound_notice">Leave the account ID blank to let each sync choose a random enabled account.</span></div><form id="source-create"><label><span class="label-row"><span data-i18n="source.book_id">Book ID</span><span class="label-hint" data-i18n="source.book_id_hint">Optional with an article URL</span></span><input name="book_id" type="text" autocomplete="off" data-i18n-placeholder="source.book_id_placeholder" placeholder="e.g. MP_WXS_2103095721"></label><label><span data-i18n="source.name">Name</span><input name="display_name" type="text" autocomplete="off" data-i18n-placeholder="source.name_placeholder" placeholder="Defaults to the resolved account name"></label><label><span data-i18n="source.article_url">Article URL</span><input name="article_url" type="url" autocomplete="url" data-i18n-placeholder="source.article_url_placeholder" placeholder="https://mp.weixin.qq.com/s/…"></label><label><span class="label-row"><span data-i18n="source.account_id">WeRead account ID</span><span class="label-hint" data-i18n="source.account_id_hint">Optional</span></span><input name="account_id" type="text" autocomplete="off" data-i18n-placeholder="source.account_id_placeholder" placeholder="Pin this source to one account"></label><div class="form-actions"><button class="button-primary" type="submit" data-i18n="source.add">Add source</button></div></form><p id="error" class="feedback" role="alert" hidden></p></section><section class="card full-width" id="source-list"><div class="card-header"><div><h2 data-i18n="source.list_heading">Sources</h2><p data-i18n="source.list_description">Monitor scheduling gates and create public feed links.</p></div><a class="button button-quiet" href="#create" data-i18n="source.new">New source</a></div><div id="sources" class="resource-grid" aria-live="polite" aria-busy="true"><div class="loading-state" data-i18n="state.loading_sources">Loading sources…</div></div></section><section class="card full-width" id="account-list"><div class="card-header"><div><h2 data-i18n="account.list_heading">WeRead accounts</h2><p data-i18n="account.list_description">Enabled accounts are available to unbound source-sync jobs.</p></div><a class="button button-quiet" href="/admin/weread/accounts" data-i18n="account.view_all">View all</a></div><div id="weread-accounts" class="resource-grid" aria-live="polite" aria-busy="true"><div class="loading-state" data-i18n="state.loading_accounts">Loading accounts…</div></div><p id="account-list-error" class="feedback" role="alert" hidden></p></section></div></main>
<script>
__I18N__
const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const list=document.querySelector('#sources');const error=document.querySelector('#error');const accountResult=document.querySelector('#account-result');const accountList=document.querySelector('#weread-accounts');const accountListError=document.querySelector('#account-list-error');const sourceCount=document.querySelector('#source-count');const activeSourceCount=document.querySelector('#active-source-count');const accountCount=document.querySelector('#account-count');
async function request(path,options={}){return fetch(path,{...options,headers:{...headers,...(options.headers||{})}})}
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return t(fallback)}
function feedback(target,message,kind='error'){target.textContent=message;target.className=`feedback ${kind}`;target.hidden=!message}
function stateMessage(message,kind='loading'){const item=document.createElement('div');item.className=`${kind}-state`;item.textContent=message;return item}
function button(label,kind,handler){const item=document.createElement('button');item.type='button';item.className=`button ${kind}`;item.textContent=label;item.addEventListener('click',handler);return item}
function chip(label,kind){const item=document.createElement('span');item.className=`status-chip ${kind}`;item.textContent=label;return item}
function meta(label,value){const row=document.createElement('div');row.className='resource-meta-row';const name=document.createElement('span');name.textContent=label;const content=document.createElement('strong');content.textContent=value;row.append(name,content);return row}
function renderSource(source){
  const item=document.createElement('article');item.className='resource-card';
  const header=document.createElement('div');header.className='resource-card-header';
  const title=document.createElement('h3');title.textContent=source.display_name;
  header.append(title,chip(t(source.enabled?'status.enabled':'status.paused'),source.enabled?'enabled':'paused'));item.append(header);
  const details=document.createElement('div');details.className='resource-meta';details.append(meta(t('source.book_id'),source.book_id),meta(t('source.scheduling'),statusLabel(source.scheduling_gate)));item.append(details);
  const actions=document.createElement('div');actions.className='resource-actions';
  const edit=document.createElement('a');edit.className='button button-quiet';edit.href=`/admin/sources/${encodeURIComponent(source.id)}`;edit.textContent=t('source.edit');actions.append(edit);
  const mutate=async(action,fallback)=>{try{const response=await action();if(!response.ok){feedback(error,await apiErrorMessage(response,fallback));return null}return response}catch{feedback(error,t('common.unreachable'));return null}};
  const toggle=button(t(source.enabled?'action.pause':'action.enable'),'button-quiet',async()=>{toggle.disabled=true;const response=await mutate(()=>request(`/api/admin/sources/${source.id}/enabled`,{method:'POST',body:JSON.stringify({enabled:!source.enabled})}),'common.source_status_failed');if(response){await load()}toggle.disabled=false});actions.append(toggle);
  const gate=button(t(source.scheduling_gate==='ready'?'source.gate_ready':'source.clear_gate'),'button-quiet',async()=>{gate.disabled=true;const response=await mutate(()=>request(`/api/admin/sources/${source.id}/gate`,{method:'POST',body:JSON.stringify({gate:'ready'})}),'common.source_gate_failed');if(response){await load()}gate.disabled=false});gate.disabled=source.scheduling_gate==='ready';actions.append(gate);
  const spacer=document.createElement('span');spacer.className='spacer';actions.append(spacer);
  const token=button(t('source.create_feed_link'),'button-secondary',async()=>{token.disabled=true;feedback(error,'');const response=await mutate(()=>request(`/api/admin/sources/${source.id}/feed-token`,{method:'POST'}),'common.feed_link_failed');if(response){try{const value=await response.json();const href=value.feed_url||value.feed_path;if(!href){throw new Error('missing feed URL')}const link=document.createElement('a');link.href=href;link.textContent=href;link.target='_blank';link.rel='noreferrer';feedResult.replaceChildren(link);feedResult.hidden=false}catch{feedback(error,t('common.feed_response_invalid'))}}token.disabled=false});actions.append(token);item.append(actions);
  const feedResult=document.createElement('p');feedResult.className='feedback success feed-result';feedResult.hidden=true;item.append(feedResult);
  const history=document.createElement('details');history.className='history';const summary=document.createElement('summary');summary.textContent=t('source.history');history.append(summary);const historyList=document.createElement('ul');history.append(historyList);
  history.addEventListener('toggle',async()=>{if(!history.open||history.dataset.loaded){return}history.dataset.loaded='true';const message=document.createElement('li');message.textContent=t('source.history_loading');historyList.replaceChildren(message);try{const response=await fetch(`/api/admin/sources/${source.id}/sync-runs`);if(!response.ok){throw new Error('history request failed')}const runs=await response.json();historyList.replaceChildren();if(!runs.length){const empty=document.createElement('li');empty.textContent=t('source.no_history');historyList.append(empty);return}runs.forEach(run=>{const runMessage=document.createElement('li');runMessage.textContent=statusLabel(run.outcome);historyList.append(runMessage)})}catch{historyList.replaceChildren();const failure=document.createElement('li');failure.textContent=t('source.history_unavailable');historyList.append(failure)}});item.append(history);return item}
async function load(){list.setAttribute('aria-busy','true');list.replaceChildren(stateMessage(t('state.loading_sources')));try{const response=await fetch('/api/admin/sources');if(!response.ok){throw new Error('source list failed')}const sources=await response.json();sourceCount.textContent=sources.length;activeSourceCount.textContent=sources.filter(source=>source.enabled).length;if(!sources.length){list.replaceChildren(stateMessage(t('state.no_sources'),'empty'));const strong=document.createElement('strong');strong.textContent=t('state.first_feed');list.firstChild.prepend(strong)}else{list.replaceChildren(...sources.map(renderSource))}feedback(error,'')}catch{sourceCount.textContent='—';activeSourceCount.textContent='—';list.replaceChildren(stateMessage(t('state.sources_load_failed'),'error'));feedback(error,t('state.load_sources_error'))}finally{list.setAttribute('aria-busy','false')}}
function renderAccount(account){const item=document.createElement('article');item.className='resource-card';const header=document.createElement('div');header.className='resource-card-header';const title=document.createElement('h3');title.textContent=account.display_name;header.append(title,chip(t(`status.${account.status}`),account.status==='active'?'active':account.status==='disabled'?'disabled':'warning'));item.append(header);const details=document.createElement('div');details.className='resource-meta';details.append(meta(t('account.id'),account.account_id),meta(t('account.current_status'),t(`status.${account.status}`)));item.append(details);const actions=document.createElement('div');actions.className='resource-actions';const edit=document.createElement('a');edit.className='button button-quiet';edit.href=`/admin/weread/accounts/${encodeURIComponent(account.account_id)}`;edit.textContent=t(account.status==='setup_required'?'account.setup_required_manage_action':'account.manage_account');actions.append(edit);item.append(actions);return item}
async function loadAccounts(){accountList.setAttribute('aria-busy','true');accountList.replaceChildren(stateMessage(t('state.loading_accounts')));try{const response=await fetch('/api/admin/weread/accounts');if(!response.ok){throw new Error('account list failed')}const accounts=await response.json();accountCount.textContent=accounts.length;if(!accounts.length){accountList.replaceChildren(stateMessage(t('state.no_accounts'),'empty'));const strong=document.createElement('strong');strong.textContent=t('state.add_account_hint');accountList.firstChild.prepend(strong)}else{accountList.replaceChildren(...accounts.map(renderAccount))}feedback(accountListError,'')}catch{accountCount.textContent='—';accountList.replaceChildren(stateMessage(t('state.accounts_load_failed'),'error'));feedback(accountListError,t('state.load_accounts_error'))}finally{accountList.setAttribute('aria-busy','false')}}
document.querySelector('#source-create').addEventListener('submit', async event => {
  event.preventDefault();
  const submit = event.target.querySelector('button[type="submit"]');
  submit.disabled = true;
  feedback(error, '');
  const form = new FormData(event.target);
  const account = form.get('account_id');
  const value = name => {
    const field = form.get(name);
    return field && field.trim() ? field.trim() : null;
  };
  try {
    const response = await request('/api/admin/sources', {
      method: 'POST',
      body: JSON.stringify({
        book_id: value('book_id'),
        display_name: value('display_name'),
        article_url: value('article_url'),
        account_id: account ? account : null,
      }),
    });
    if (response.ok) {
      event.target.reset();
      await load();
      feedback(error, t('common.source_added'), 'success');
    } else {
      feedback(error, await apiErrorMessage(response, 'common.source_add_failed'));
    }
  } catch {
    feedback(error, t('common.unreachable'));
  } finally {
    submit.disabled = false;
  }
});
document.querySelector('#weread-account').addEventListener('submit',async event=>{event.preventDefault();const submit=event.target.querySelector('button[type="submit"]');submit.disabled=true;feedback(accountResult,'');const form=new FormData(event.target);const account=form.get('account_id');const displayName=form.get('display_name');const path=account?`/api/admin/weread/accounts/${encodeURIComponent(account)}`:'/api/admin/weread/accounts';try{const response=await request(path,{method:account?'PUT':'POST',body:JSON.stringify({account_id:account?account:null,display_name:displayName&&displayName.trim()?displayName.trim():null,cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){const value=await response.json();event.target.reset();feedback(accountResult,t('common.account_saved_detail').replace('{id}',value.account_id),'success');await loadAccounts()}else{feedback(accountResult,await apiErrorMessage(response,'common.account_save_failed'))}}catch{feedback(accountResult,t('common.unreachable'))}finally{submit.disabled=false}});
document.querySelector('#logout').addEventListener('click',async()=>{const logout=document.querySelector('#logout');logout.disabled=true;try{await request('/api/admin/logout',{method:'POST'})}finally{location='/admin/login'}});load();loadAccounts();
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__LOCALE_PICKER__", LOCALE_PICKER)
            .replace(
                "<div class=\"layout-grid\">",
                &format!("<div class=\"layout-grid\">{CONFIG_TRANSFER_CARD}"),
            )
            .replace("__USERNAME__", &username)
            .replace("__CSRF__", &csrf_json)
            .replace("__I18N__", &i18n_bootstrap(locale))
            .replace(
                r#"<section class="card" id="create">"#,
                &format!("{QR_LOGIN_CARD}<section class=\"card\" id=\"create\">"),
            )
            .replace(
                "</script></body></html>",
                &format!("{CONFIG_TRANSFER_SCRIPT}{QR_LOGIN_SCRIPT}</script></body></html>"),
            ),
    )
}

/// Renders the authenticated source configuration and lifecycle page.
pub fn source_page(session: &AdminSession, source: &Source, locale: Locale) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let source_id = escape_html(&source.id().to_string());
    let book_id = escape_html(source.book_id());
    let display_name = escape_html(source.display_name());
    let article_url = escape_html(
        source
            .article_url()
            .map(ToString::to_string)
            .unwrap_or_default()
            .as_str(),
    );
    let account_id = escape_html(
        source
            .account_id()
            .map(|value| value.to_string())
            .unwrap_or_default()
            .as_str(),
    );
    let (status_key, status_text, toggle_key, toggle_text) = if source.enabled() {
        ("status.enabled", "Enabled", "source.pause_source", "Pause")
    } else {
        ("status.paused", "Paused", "source.enable_source", "Enable")
    };
    let status_class = if source.enabled() {
        "enabled"
    } else {
        "paused"
    };
    let (gate_key, gate_text) = match source.scheduling_gate() {
        crate::domain::source::SchedulingGate::Ready => ("status.ready", "Ready"),
        crate::domain::source::SchedulingGate::AuthenticationRequired => {
            ("status.authentication_required", "Authentication required")
        }
        crate::domain::source::SchedulingGate::RiskControlled => {
            ("status.risk_controlled", "Risk controlled")
        }
    };
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title data-i18n="source.edit_title">Edit source — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small data-i18n="app.admin_console">Admin console</small></span></a><div class="header-actions"><span class="identity"><span data-i18n="nav.signed_in_as">Signed in as</span> <strong>__USERNAME__</strong></span><a class="button button-quiet" href="/admin/" data-i18n="nav.dashboard">Dashboard</a>__LOCALE_PICKER__</div></div></header><main class="page-shell"><nav class="breadcrumb" data-i18n-aria-label="common.breadcrumb" aria-label="Breadcrumb"><a href="/admin/" data-i18n="nav.dashboard">Dashboard</a><span>/</span><span data-i18n="source.edit_heading">Edit source</span></nav><section class="page-header"><div><p class="kicker" data-i18n="source.settings_kicker">Source settings</p><h1 data-i18n="source.edit_heading">Edit source</h1><p data-i18n="source.edit_description">Update how this source is identified, scheduled, and delivered.</p></div><span class="status-chip __STATUS_CLASS__" data-i18n="__STATUS_KEY__">__STATUS_TEXT__</span></section><div class="split-panel"><section class="card"><div class="card-header"><div><h2 data-i18n="source.configuration">Configuration</h2><p data-i18n="source.config_description">Changes take effect on the next synchronization cycle.</p></div><span class="card-icon" aria-hidden="true">⚙</span></div><form id="source"><fieldset class="form-section"><legend data-i18n="source.feed_identity">Feed identity</legend><div class="field-grid"><label><span data-i18n="source.book_id">Book ID</span><input name="book_id" value="__BOOK_ID__" required></label><label><span data-i18n="source.name">Name</span><input name="display_name" value="__DISPLAY_NAME__" required></label><label class="wide"><span class="label-row"><span data-i18n="source.article_url">Article URL</span><span class="label-hint" data-i18n="source.article_url_hint">Optional for Book ID-only sources</span></span><input name="article_url" type="url" value="__ARTICLE_URL__"><span class="small" data-i18n="source.article_url_help">Clear it when the source is identified only by Book ID.</span></label><label class="wide"><span class="label-row"><span data-i18n="source.account_id">WeRead account ID</span><span class="label-hint" data-i18n="source.account_id_hint">Optional</span></span><input name="account_id" value="__ACCOUNT_ID__"><span class="small" data-i18n="source.account_id_help">Clear it to let the worker choose an enabled account.</span></label></div></fieldset><fieldset class="form-section"><legend data-i18n="source.delivery_policy">Delivery policy</legend><div class="field-grid"><label><span data-i18n="source.sync_interval">Sync interval (seconds)</span><input name="sync_interval_seconds" type="number" min="1" value="__SYNC_INTERVAL__" required></label><label><span data-i18n="source.rss_item_limit">RSS item limit</span><input name="rss_item_limit" type="number" min="1" value="__RSS_ITEM_LIMIT__" required></label><label><span data-i18n="source.priority">Priority</span><input name="priority" type="number" value="__PRIORITY__" required></label><label><span data-i18n="source.maximum_attempts">Maximum attempts</span><input name="max_attempts" type="number" min="1" value="__MAX_ATTEMPTS__" required></label></div></fieldset><div class="form-actions"><a class="button button-quiet" href="/admin/" data-i18n="action.cancel">Cancel</a><button class="button-primary" type="submit" data-i18n="action.save_changes">Save changes</button></div></form><p id="result" class="feedback" role="status" hidden></p></section><aside class="side-stack"><section class="card"><div class="card-header"><div><h2 data-i18n="source.runtime_status">Runtime status</h2><p data-i18n="source.runtime_status_description">Current scheduling state.</p></div><span class="card-icon green" aria-hidden="true">✓</span></div><dl class="definition-list"><div><dt data-i18n="source.status">Status</dt><dd data-i18n="__STATUS_KEY__">__STATUS_TEXT__</dd></div><div><dt data-i18n="source.gate">Gate</dt><dd>__GATE__</dd></div><div><dt data-i18n="source.revision">Revision</dt><dd>__REVISION__</dd></div></dl><div class="form-actions"><button id="toggle" class="button-secondary" type="button"><span data-i18n="__TOGGLE_KEY__">__TOGGLE_TEXT__</span> <span data-i18n="common.source">source</span></button><button id="clear-gate" class="button-quiet" type="button" data-i18n="source.clear_gate">Clear gate</button></div></section><section class="card danger-zone"><div class="card-header"><div><h2 data-i18n="source.danger_zone">Danger zone</h2><p data-i18n="source.delete_description">Deleting removes this source and its stored articles.</p></div><span class="card-icon red" aria-hidden="true">!</span></div><button id="delete" class="button-danger" type="button" data-i18n="action.delete_source">Delete source</button></section></aside></div></main>
<script>
__I18N__
const sourceId='__SOURCE_ID__';const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const result=document.querySelector('#result');
const request=(path,options={})=>fetch(path,{...options,headers:{...headers,...(options.headers||{})}});
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return t(fallback)}
async function runControlAction(control,action,fallback){control.disabled=true;try{const response=await action();if(response.ok){return true}result.textContent=await apiErrorMessage(response,fallback)}catch{result.textContent=t('common.unreachable')}finally{control.disabled=false}result.className='feedback error';result.hidden=false;return false}
document.querySelector('#source').addEventListener('submit',async event=>{event.preventDefault();const submit=event.target.querySelector('button[type="submit"]');submit.disabled=true;result.hidden=true;const form=new FormData(event.target);const value=name=>{const field=form.get(name);return field&&field.toString().trim()?field.toString().trim():null};try{const response=await request(`/api/admin/sources/${sourceId}`,{method:'PUT',body:JSON.stringify({book_id:value('book_id'),display_name:value('display_name'),article_url:value('article_url'),account_id:value('account_id'),sync_interval_seconds:Number(form.get('sync_interval_seconds')),rss_item_limit:Number(form.get('rss_item_limit')),priority:Number(form.get('priority')),max_attempts:Number(form.get('max_attempts'))})});if(response.ok){result.textContent=t('source.updated');result.className='feedback success';result.hidden=false;setTimeout(()=>location.reload(),350)}else{result.textContent=await apiErrorMessage(response,'source.update_failed');result.className='feedback error';result.hidden=false}}catch{result.textContent=t('common.unreachable');result.className='feedback error';result.hidden=false}finally{submit.disabled=false}});
document.querySelector('#toggle').addEventListener('click',async()=>{const control=document.querySelector('#toggle');if(await runControlAction(control,()=>request(`/api/admin/sources/${sourceId}/enabled`,{method:'POST',body:JSON.stringify({enabled:__NEXT_ENABLED__})}),'source.status_change_failed')){location.reload()}});
document.querySelector('#clear-gate').addEventListener('click',async()=>{const control=document.querySelector('#clear-gate');if(await runControlAction(control,()=>request(`/api/admin/sources/${sourceId}/gate`,{method:'POST',body:JSON.stringify({gate:'ready'})}),'source.gate_clear_failed')){location.reload()}});
document.querySelector('#delete').addEventListener('click',async()=>{if(!confirm(t('source.delete_confirm'))){return}const control=document.querySelector('#delete');if(await runControlAction(control,()=>request(`/api/admin/sources/${sourceId}`,{method:'DELETE'}),'source.delete_failed')){location='/admin/'}});
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__LOCALE_PICKER__", LOCALE_PICKER)
            .replace("__USERNAME__", &username)
            .replace("__SOURCE_ID__", &source_id)
            .replace("__BOOK_ID__", &book_id)
            .replace("__DISPLAY_NAME__", &display_name)
            .replace("__ARTICLE_URL__", &article_url)
            .replace("__ACCOUNT_ID__", &account_id)
            .replace("__STATUS_KEY__", status_key)
            .replace("__STATUS_TEXT__", status_text)
            .replace("__STATUS_CLASS__", status_class)
            .replace(
                "<dd>__GATE__</dd>",
                r#"<dd data-i18n="__GATE_KEY__">__GATE_TEXT__</dd>"#,
            )
            .replace("__GATE_KEY__", gate_key)
            .replace("__GATE_TEXT__", gate_text)
            .replace("__REVISION__", &source.feed_revision().to_string())
            .replace(
                "__SYNC_INTERVAL__",
                &source.sync_interval().num_seconds().to_string(),
            )
            .replace("__RSS_ITEM_LIMIT__", &source.rss_item_limit().to_string())
            .replace("__PRIORITY__", &source.priority().to_string())
            .replace("__MAX_ATTEMPTS__", &source.max_attempts().to_string())
            .replace("__TOGGLE_KEY__", toggle_key)
            .replace("__TOGGLE__", toggle_text)
            .replace(
                "__NEXT_ENABLED__",
                if source.enabled() { "false" } else { "true" },
            )
            .replace("__CSRF__", &csrf_json)
            .replace("__I18N__", &i18n_bootstrap(locale)),
    )
}

/// Renders the authenticated WeRead account list page.
pub fn weread_accounts_page(session: &AdminSession, locale: Locale) -> Html<String> {
    let username = escape_html(session.username());
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title data-i18n="account.accounts_title">WeRead accounts — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small data-i18n="app.admin_console">Admin console</small></span></a><div class="header-actions"><span class="identity"><span data-i18n="nav.signed_in_as">Signed in as</span> <strong>__USERNAME__</strong></span><a class="button button-quiet" href="/admin/" data-i18n="nav.dashboard">Dashboard</a>__LOCALE_PICKER__</div></div></header><main class="page-shell"><nav class="breadcrumb" data-i18n-aria-label="common.breadcrumb" aria-label="Breadcrumb"><a href="/admin/" data-i18n="nav.dashboard">Dashboard</a><span>/</span><span data-i18n="account.list_heading">WeRead accounts</span></nav><section class="page-header"><div><p class="kicker" data-i18n="account.credentials_kicker">Credentials</p><h1 data-i18n="account.list_heading">WeRead accounts</h1><p data-i18n="account.page_description">Keep authenticated browser sessions healthy without exposing stored cookies.</p></div><a class="button button-primary" href="/admin/#weread-account-card" data-i18n="action.add_account">Add account</a></section><section class="card"><div class="card-header"><div><h2 data-i18n="account.directory">Account directory</h2><p data-i18n="account.directory_description">Active accounts can be selected for unbound source synchronization.</p></div><span class="card-icon green" aria-hidden="true">W</span></div><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span data-i18n="account.credentials_notice">Credentials are encrypted at rest. Manage an account to replace its cookie header or change its status.</span></div><p id="error" class="feedback" role="alert" hidden></p><div id="accounts" class="resource-grid" aria-live="polite" aria-busy="true"><div class="loading-state" data-i18n="state.loading_accounts">Loading accounts…</div></div></section></main>
<script>
__I18N__
const list=document.querySelector('#accounts');const error=document.querySelector('#error');
function stateMessage(message,kind='loading'){const item=document.createElement('div');item.className=`${kind}-state`;item.textContent=message;return item}
async function loadAccounts(){list.setAttribute('aria-busy','true');try{const response=await fetch('/api/admin/weread/accounts');if(!response.ok){throw new Error('account list failed')}const accounts=await response.json();if(!accounts.length){list.replaceChildren(stateMessage(t('state.no_accounts_page'),'empty'));const strong=document.createElement('strong');strong.textContent=t('state.add_first_account');list.firstChild.prepend(strong);return}list.replaceChildren(...accounts.map(account=>{const item=document.createElement('article');item.className='resource-card';const header=document.createElement('div');header.className='resource-card-header';const title=document.createElement('h3');title.textContent=account.display_name;const status=document.createElement('span');status.className=`status-chip ${account.status==='active'?'active':account.status==='disabled'?'disabled':'warning'}`;status.textContent=statusLabel(account.status);header.append(title,status);item.append(header);const details=document.createElement('div');details.className='resource-meta';const row=document.createElement('div');row.className='resource-meta-row';const label=document.createElement('span');label.textContent=t('account.id');const value=document.createElement('strong');value.textContent=account.account_id;row.append(label,value);details.append(row);item.append(details);const edit=document.createElement('a');edit.className='button button-quiet';edit.href=`/admin/weread/accounts/${encodeURIComponent(account.account_id)}`;edit.textContent=t(account.status==='setup_required'?'account.setup_required_manage_action':'account.manage_account');item.append(edit);return item}))}catch{list.replaceChildren(stateMessage(t('state.accounts_page_load_failed'),'error'));error.textContent=t('state.accounts_page_error');error.className='feedback error';error.hidden=false}finally{list.setAttribute('aria-busy','false')}}
loadAccounts();
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__LOCALE_PICKER__", LOCALE_PICKER)
            .replace("__USERNAME__", &username)
            .replace("__I18N__", &i18n_bootstrap(locale)),
    )
}

/// Renders the account-specific credential replacement page.
pub fn weread_account_page(
    session: &AdminSession,
    account: &WeReadAccount,
    locale: Locale,
) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let account_id = account.account_id().to_string();
    let display_name = escape_html(account.display_name());
    let expires_at = account
        .access_expires_at()
        .map(|value| escape_html(&value.to_rfc3339()))
        .unwrap_or_default();
    let status = if account.setup_required() {
        "setup_required"
    } else if account.disabled() {
        "disabled"
    } else {
        "active"
    };
    let status_class = if account.setup_required() {
        "warning"
    } else {
        status
    };
    let setup_required = account.setup_required();
    let (toggle_key, toggle_text) = if account.disabled() {
        ("account.enable_account", "Enable")
    } else {
        ("account.disable_account", "Disable")
    };
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title data-i18n="account.account_title">WeRead account — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small data-i18n="app.admin_console">Admin console</small></span></a><div class="header-actions"><span class="identity"><span data-i18n="nav.signed_in_as">Signed in as</span> <strong>__USERNAME__</strong></span><a class="button button-quiet" href="/admin/weread/accounts" data-i18n="nav.accounts">Accounts</a>__LOCALE_PICKER__</div></div></header><main class="page-shell"><nav class="breadcrumb" data-i18n-aria-label="common.breadcrumb" aria-label="Breadcrumb"><a href="/admin/" data-i18n="nav.dashboard">Dashboard</a><span>/</span><a href="/admin/weread/accounts" data-i18n="account.list_heading">WeRead accounts</a><span>/</span><span data-i18n="nav.manage_account">Manage account</span></nav><section class="page-header"><div><p class="kicker" data-i18n="account.credential_settings_kicker">Credential settings</p><h1 data-i18n="account.manage_heading">Manage WeRead account</h1><p data-i18n="account.manage_description">Rotate the browser session or update its display name.</p></div><span class="status-chip __STATUS_CLASS__" data-i18n="status.__STATUS__">__STATUS__</span></section><div class="split-panel"><section class="card"><div class="card-header"><div><h2 data-i18n="account.details">Account details</h2><p><span data-i18n="account.id">Account ID</span> <code>__ACCOUNT_ID__</code></p></div><span class="card-icon green" aria-hidden="true">W</span></div><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span data-i18n="account.rotation_notice">For security, the stored cookie is never displayed. Paste a complete fresh Cookie request-header value when rotating credentials.</span></div><form id="account"><label><span class="label-row"><span data-i18n="account.display_name">Display name</span><span class="label-hint" data-i18n="account.display_name_hint">Optional when wr_name is present</span></span><input name="display_name" value="__DISPLAY_NAME__"><span class="small" data-i18n="account.display_name_help">You may leave this blank if the cookie contains a usable <code>wr_name</code>.</span></label><label><span data-i18n="account.new_cookie_header">New WeRead Cookie header</span><textarea name="cookie_header" rows="7" required autocomplete="off" data-i18n-placeholder="account.cookie_placeholder" placeholder="wr_vid=…; wr_skey=…; wr_rt=…"></textarea></label><label><span data-i18n="account.access_expiry">Access token expiry</span><input id="expiry" name="access_expires_at" type="datetime-local" data-value="__EXPIRES_AT__" required></label><div class="form-actions"><a class="button button-quiet" href="/admin/weread/accounts" data-i18n="action.cancel">Cancel</a><button class="button-primary" type="submit" data-i18n="action.save_changes">Save changes</button></div></form><p id="result" class="feedback" role="status" hidden></p></section><aside class="side-stack"><section class="card"><div class="card-header"><div><h2 data-i18n="account.account_status">Account status</h2><p data-i18n="account.disabled_help">Disabled accounts are skipped by random selection.</p></div><span class="card-icon orange" aria-hidden="true">●</span></div><button id="toggle" class="button-secondary" type="button" data-i18n="account.__TOGGLE__">__TOGGLE__ account</button></section><section class="card danger-zone"><div class="card-header"><div><h2 data-i18n="source.danger_zone">Danger zone</h2><p data-i18n="account.delete_description">Deleting removes the stored credentials permanently.</p></div><span class="card-icon red" aria-hidden="true">!</span></div><button id="delete" class="button-danger" type="button" data-i18n="action.delete_account">Delete account</button></section></aside></div></main>
<script>
__I18N__
const accountId='__ACCOUNT_ID__';const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const result=document.querySelector('#result');const expiry=document.querySelector('#expiry');if(expiry.dataset.value){expiry.value=new Date(expiry.dataset.value).toISOString().slice(0,16)};
const request=(path,options={})=>fetch(path,{...options,headers:{...headers,...(options.headers||{})}});
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return t(fallback)}
async function runControlAction(control,action,fallback){control.disabled=true;try{const response=await action();if(response.ok){return true}result.textContent=await apiErrorMessage(response,fallback)}catch{result.textContent=t('common.unreachable')}finally{control.disabled=false}result.className='feedback error';result.hidden=false;return false}
document.querySelector('#account').addEventListener('submit',async event=>{event.preventDefault();const submit=event.target.querySelector('button[type="submit"]');submit.disabled=true;result.hidden=true;const form=new FormData(event.target);try{const response=await request(`/api/admin/weread/accounts/${accountId}`,{method:'PUT',body:JSON.stringify({account_id:accountId,display_name:form.get('display_name')&&form.get('display_name').trim()?form.get('display_name').trim():null,cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){result.textContent=t('account.updated');result.className='feedback success';result.hidden=false;setTimeout(()=>location.reload(),350)}else{result.textContent=await apiErrorMessage(response,'account.update_failed');result.className='feedback error';result.hidden=false}}catch{result.textContent=t('common.unreachable');result.className='feedback error';result.hidden=false}finally{submit.disabled=false}});
document.querySelector('#toggle').addEventListener('click',async()=>{const control=document.querySelector('#toggle');if(await runControlAction(control,()=>request(`/api/admin/weread/accounts/${accountId}/enabled`,{method:'POST',body:JSON.stringify({enabled:__ENABLED__})}),'account.status_change_failed')){location.reload()}});
document.querySelector('#delete').addEventListener('click',async()=>{if(!confirm(t('account.delete_confirm'))){return}const control=document.querySelector('#delete');if(await runControlAction(control,()=>request(`/api/admin/weread/accounts/${accountId}`,{method:'DELETE'}),'account.delete_failed')){location='/admin/'}});
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__LOCALE_PICKER__", LOCALE_PICKER)
            .replace("__USERNAME__", &username)
            .replace("__ACCOUNT_ID__", &account_id)
            .replace("__STATUS__", status)
            .replace("__STATUS_CLASS__", status_class)
            .replace("__DISPLAY_NAME__", &display_name)
            .replace("__EXPIRES_AT__", &expires_at)
            .replace(
                "<span class=\"notice-icon\" aria-hidden=\"true\">i</span><span data-i18n=\"account.rotation_notice\">For security, the stored cookie is never displayed. Paste a complete fresh Cookie request-header value when rotating credentials.</span>",
                if setup_required {
                    "<span class=\"notice-icon\" aria-hidden=\"true\">i</span><span data-i18n=\"account.setup_required_notice\">This account needs WeRead credentials before it can be used. Enroll a fresh cookie header below.</span>"
                } else {
                    "<span class=\"notice-icon\" aria-hidden=\"true\">i</span><span data-i18n=\"account.rotation_notice\">For security, the stored cookie is never displayed. Paste a complete fresh Cookie request-header value when rotating credentials.</span>"
                },
            )
            .replace(
                "data-i18n=\"account.manage_description\">Rotate the browser session or update its display name.",
                if setup_required {
                    "data-i18n=\"account.setup_required_manage_description\">Enroll credentials to activate this account."
                } else {
                    "data-i18n=\"account.manage_description\">Rotate the browser session or update its display name."
                },
            )
            .replace(
                r#"<span class="small" data-i18n="account.display_name_help">You may leave this blank if the cookie contains a usable <code>wr_name</code>.</span>"#,
                r#"<span class="small"><span data-i18n="account.display_name_help_before">You may leave this blank if the cookie contains a usable </span><code>wr_name</code><span data-i18n="account.display_name_help_after">.</span></span>"#,
            )
            .replace("account.__TOGGLE__", toggle_key)
            .replace("__TOGGLE__", toggle_text)
            .replace(
                "__ENABLED__",
                if account.disabled() { "true" } else { "false" },
            )
            .replace("__CSRF__", &csrf_json)
            .replace("__I18N__", &i18n_bootstrap(locale)),
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::auth::AdminAuthenticator;
    use crate::web::i18n::Locale;
    use secrecy::SecretString;

    fn session_for(username: &str) -> AdminSession {
        let auth = AdminAuthenticator::new(
            username.to_owned(),
            SecretString::new("password".to_owned().into_boxed_str()),
            SecretString::new("signing-key".to_owned().into_boxed_str()),
        )
        .unwrap();
        auth.login(
            username,
            "password",
            "test",
            "2026-09-01T00:00:00Z".parse().unwrap(),
        )
        .unwrap()
        .0
    }

    fn session() -> AdminSession {
        session_for("admin")
    }

    #[test]
    fn login_page_has_accessible_sign_in_form_and_responsive_styles() {
        let body = login_page(Locale::English).0;
        assert!(body.contains("<html lang=\"en\">"));
        assert!(body.contains("autocomplete=\"username\""));
        assert!(body.contains("autocomplete=\"current-password\""));
        assert!(body.contains("prefers-reduced-motion"));
    }

    #[test]
    fn admin_page_embeds_french_translations_and_language_switcher() {
        let body = admin_page(&session(), Locale::French).0;

        assert!(body.contains("const translations={"));
        assert!(body.contains("Ravi de vous revoir。"));
        assert!(body.contains("id=\"locale\""));
        assert!(body.contains("werrss_locale="));
        assert!(!body.contains("__I18N__"));
        assert!(!body.contains("__LOCALE_PICKER__"));
        assert!(!body.contains("createTreeWalker"));
        assert!(!body.contains("textKeys"));
    }

    #[test]
    fn login_page_embeds_chinese_translations() {
        let body = login_page(Locale::Chinese).0;

        assert!(body.contains("document.documentElement.lang='zh'"));
        assert!(body.contains("欢迎回来"));
        assert!(body.contains("保存后不会显示敏感信息"));
    }

    #[test]
    fn admin_page_renders_navigation_and_workspace_summary() {
        let body = admin_page(&session(), Locale::English).0;
        assert!(body.contains("href=\"/admin/weread/accounts\""));
        assert!(body.contains("id=\"source-count\""));
        assert!(body.contains("id=\"active-source-count\""));
        assert!(body.contains("id=\"account-count\""));
        assert!(body.contains("id=\"sources\" class=\"resource-grid\""));
        assert!(body.contains("id=\"weread-accounts\" class=\"resource-grid\""));
    }

    #[test]
    fn admin_page_renders_configuration_export_import_controls() {
        let body = admin_page(&session(), Locale::English).0;

        assert!(body.contains("id=\"configuration\""));
        assert!(body.contains("data-i18n=\"config.heading\">Configuration backup</h2>"));
        assert!(body.contains("<label for=\"config-export-sources\">"));
        assert!(body.contains("<label for=\"config-export-accounts\">"));
        assert!(body.contains("id=\"config-export-sources\" name=\"sources\" type=\"checkbox\""));
        assert!(body
            .contains("id=\"config-export-accounts\" name=\"weread_accounts\" type=\"checkbox\""));
        assert!(body.contains("id=\"config-file\" type=\"file\" accept=\"application/json,.json\""));
        assert!(body.contains("id=\"config-preview\""));
        assert!(body.contains(
            "id=\"config-apply\" class=\"button button-primary\" type=\"button\" disabled"
        ));
        assert!(body.contains("request('/api/admin/config/export?'+query)"));
        assert!(body.contains("new URLSearchParams({sources:String(configSources.checked),weread_accounts:String(configAccounts.checked)})"));
        assert!(body.contains("/api/admin/config/import/preview"));
        assert!(body.contains("/api/admin/config/import'"));
        assert!(body.contains("conflicts.length===0"));
        assert!(
            body.contains("Articles, synchronization history, and cached assets are not included")
        );
        for field in [
            "sources_created",
            "sources_updated",
            "accounts_created",
            "accounts_updated",
            "placeholders_created",
            "account_bindings_preserved",
            "account_bindings_cleared",
        ] {
            assert!(body.contains(field), "missing result field: {field}");
        }
        assert!(body.contains("'x-csrf-token':csrf"));
    }

    #[test]
    fn admin_page_embeds_configuration_copy_in_french_and_chinese() {
        let french = admin_page(&session(), Locale::French).0;
        assert!(french.contains("Sauvegarde de la configuration"));
        assert!(french.contains("Les articles, l'historique de synchronisation et les ressources en cache ne sont pas inclus。"));
        assert!(french.contains("Configurer les identifiants"));

        let chinese = admin_page(&session(), Locale::Chinese).0;
        assert!(chinese.contains("配置备份"));
        assert!(chinese.contains("不会包含文章、同步历史记录和缓存资源。"));
        assert!(chinese.contains("设置凭据"));
    }

    #[test]
    fn admin_page_exposes_the_qr_login_form_and_lifecycle_script() {
        let body = admin_page(&session(), Locale::English).0;
        assert!(body.contains("id=\"weread-qr-card\""));
        assert!(body.contains("id=\"weread-qr\""));
        assert!(body.contains("/api/admin/weread/qr"));
        assert!(body.contains("const response=await request('/api/admin/weread/qr/'"));
        assert!(body.contains("data:image/svg+xml"));
        assert!(body.contains("clearTimeout(qrTimer)"));
        assert!(body.contains("qrAttemptId||qrStartInFlight"));
        assert!(body.contains("pollQr(value.attempt_id,generation)"));
        assert!(body.contains("generation!==qrPollGeneration||attemptId!==qrAttemptId"));
    }

    #[test]
    fn admin_page_escapes_username_in_the_navigation() {
        let body = admin_page(&session_for("<admin> & operator"), Locale::English).0;
        assert!(body.contains("&lt;admin&gt; &amp; operator"));
        assert!(!body.contains("<admin> & operator"));
    }

    #[test]
    fn admin_page_does_not_translate_user_supplied_names() {
        let body = admin_page(&session_for("Sources"), Locale::French).0;

        assert!(body.contains("<strong>Sources</strong>"));
    }

    #[test]
    fn admin_page_keeps_credentials_out_of_markup_and_uses_api_error_messages() {
        let body = admin_page(&session(), Locale::English).0;
        assert!(!body.contains("correct horse"));
        assert!(!body.contains("wr_skey=secret"));
        assert!(body.contains("async function apiErrorMessage(response,fallback)"));
        assert!(body.contains("typeof value.error==='string'"));
        assert!(body.contains("return t(fallback)"));
        assert!(body.contains("accountResult"));
        assert!(body.contains("/api/admin/weread/accounts"));
    }

    #[test]
    fn admin_page_uses_absolute_feed_url_when_the_api_provides_one() {
        let body = admin_page(&session(), Locale::English).0;
        assert!(body.contains("const href=value.feed_url||value.feed_path"));
        assert!(body.contains("link.target='_blank'"));
    }

    #[test]
    fn source_page_groups_configuration_and_lifecycle_controls() {
        let source = Source::new(crate::domain::source::NewSource::test_default())
            .expect("test source should be valid");
        let body = source_page(&session(), &source, Locale::English).0;
        assert!(body.contains("<h1 data-i18n=\"source.edit_heading\">Edit source</h1>"));
        assert!(body.contains("<legend data-i18n=\"source.feed_identity\">Feed identity</legend>"));
        assert!(
            body.contains("<legend data-i18n=\"source.delivery_policy\">Delivery policy</legend>")
        );
        assert!(body.contains("id=\"toggle\""));
        assert!(body.contains("id=\"clear-gate\""));
        assert!(body.contains("id=\"delete\""));
        assert!(!body.contains("name=\"cookie_header\""));
    }

    #[test]
    fn source_page_restores_lifecycle_controls_after_request_failures() {
        let source = Source::new(crate::domain::source::NewSource::test_default())
            .expect("test source should be valid");
        let body = source_page(&session(), &source, Locale::English).0;

        assert!(body.contains("async function runControlAction(control,action,fallback)"));
        assert!(body.contains("catch{result.textContent=t('common.unreachable')}"));
        assert!(body.contains("finally{control.disabled=false}"));
        assert_eq!(body.matches("if(await runControlAction(control").count(), 3);
    }

    #[test]
    fn source_page_exposes_non_ready_gate_as_a_translation_key() {
        let mut spec = crate::domain::source::NewSource::test_default();
        spec.scheduling_gate = crate::domain::source::SchedulingGate::AuthenticationRequired;
        let source = Source::new(spec).expect("test source should be valid");
        let body = source_page(&session(), &source, Locale::French).0;

        assert!(body.contains(
            "<dd data-i18n=\"status.authentication_required\">Authentication required</dd>"
        ));
        assert!(!body.contains("__GATE__"));
    }

    #[test]
    fn source_page_escapes_values_and_supports_unbound_sources() {
        let mut spec = crate::domain::source::NewSource::test_default();
        spec.book_id = "book<&\"".to_owned();
        spec.display_name = "Name<&\"".to_owned();
        spec.article_url = None;
        let source = Source::new(spec).expect("test source should be valid");
        let body = source_page(&session(), &source, Locale::English).0;
        assert!(body.contains("name=\"book_id\" value=\"book&lt;&amp;&quot;\""));
        assert!(body.contains("name=\"display_name\" value=\"Name&lt;&amp;&quot;\""));
        assert!(body.contains("name=\"article_url\" type=\"url\" value=\"\""));
        assert!(body.contains("name=\"account_id\" value=\"\""));
    }

    #[test]
    fn account_list_page_has_empty_state_and_safe_account_navigation() {
        let body = weread_accounts_page(&session(), Locale::English).0;
        assert!(body.contains("<h1 data-i18n=\"account.list_heading\">WeRead accounts</h1>"));
        assert!(body.contains("href=\"/admin/\""));
        assert!(body.contains("No WeRead accounts have been added."));
        assert!(body.contains("/admin/weread/accounts/${encodeURIComponent(account.account_id)}"));
        assert!(!body.contains("name=\"cookie_header\""));
    }

    #[test]
    fn setup_required_account_list_has_direct_enrollment_action() {
        let body = weread_accounts_page(&session(), Locale::English).0;

        assert!(body.contains("statusLabel(account.status)"));
        assert!(body.contains("edit.textContent=t(account.status==='setup_required'?'account.setup_required_manage_action':'account.manage_account')"));
        assert!(body.contains("/admin/weread/accounts/${encodeURIComponent(account.account_id)}"));
    }

    #[test]
    fn setup_required_account_page_shows_enrollment_form_without_expiry_value() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(2)),
            "Imported".to_owned(),
            1,
            None,
            false,
            true,
        );
        let body = weread_account_page(&session(), &account, Locale::English).0;

        assert!(body.contains("data-i18n=\"status.setup_required\">setup_required</span>"));
        assert!(body.contains("data-i18n=\"account.setup_required_notice\">"));
        assert!(body.contains("id=\"expiry\" name=\"access_expires_at\" type=\"datetime-local\" data-value=\"\" required"));
        assert!(
            body.contains("if(expiry.dataset.value){expiry.value=new Date(expiry.dataset.value)")
        );
        assert!(!body.contains("access_expires_at\">"));
    }

    #[test]
    fn disabled_setup_required_account_keeps_disabled_state_visible_as_enable_action() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(2)),
            "Imported".to_owned(),
            1,
            None,
            true,
            true,
        );
        let body = weread_account_page(&session(), &account, Locale::English).0;

        assert!(body.contains("data-i18n=\"status.setup_required\">setup_required</span>"));
        assert!(body.contains("data-i18n=\"account.enable_account\">Enable account</button>"));
        assert!(body.contains("Enroll credentials to activate this account."));
        assert!(body.contains("name=\"cookie_header\""));
    }

    #[test]
    fn account_page_restores_lifecycle_controls_after_request_failures() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            Some("2026-10-01T00:00:00Z".parse().unwrap()),
            false,
            false,
        );
        let body = weread_account_page(&session(), &account, Locale::English).0;

        assert!(body.contains("async function runControlAction(control,action,fallback)"));
        assert!(body.contains("catch{result.textContent=t('common.unreachable')}"));
        assert!(body.contains("finally{control.disabled=false}"));
        assert_eq!(body.matches("if(await runControlAction(control").count(), 2);
    }

    #[test]
    fn account_page_exposes_rotation_and_lifecycle_controls_without_credentials() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            Some("2026-10-01T00:00:00Z".parse().unwrap()),
            false,
            false,
        );
        let body = weread_account_page(&session(), &account, Locale::English).0;
        assert!(body.contains("Account details"));
        assert!(body.contains("Optional when wr_name is present"));
        assert!(body.contains("/api/admin/weread/accounts/${accountId}"));
        assert!(body.contains("/api/admin/weread/accounts/${accountId}/enabled"));
        assert!(body.contains("method:'DELETE'"));
        assert!(body.contains("data-value=\"2026-10-01T00:00:00+00:00\""));
        assert!(!body.contains("access-token"));
    }

    #[test]
    fn disabled_account_page_uses_a_stable_enable_translation_key() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            Some("2026-10-01T00:00:00Z".parse().unwrap()),
            true,
            false,
        );
        let body = weread_account_page(&session(), &account, Locale::Chinese).0;

        assert!(body.contains("data-i18n=\"account.enable_account\">Enable account</button>"));
        assert!(!body.contains("account.__TOGGLE__"));
    }

    #[test]
    fn account_display_name_help_keeps_the_cookie_name_marked_up() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            Some("2026-10-01T00:00:00Z".parse().unwrap()),
            false,
            false,
        );
        let body = weread_account_page(&session(), &account, Locale::French).0;

        assert!(body.contains(
            "<span class=\"small\"><span data-i18n=\"account.display_name_help_before\">"
        ));
        assert!(body.contains("<code>wr_name</code>"));
        assert!(body.contains("data-i18n=\"account.display_name_help_after\">.</span>"));
    }
}
