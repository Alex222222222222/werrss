//! Locale selection and translation resources for the embedded admin panel.
//!
//! The panel deliberately keeps its translation catalog in the Rust binary so
//! deployments do not need a second static-file or frontend build pipeline.
//! The browser's saved `werrss_locale` preference takes precedence over
//! `Accept-Language`; unsupported or malformed values fall back to English.

use std::cmp::Reverse;

use axum::http::{header, HeaderMap};

const LOCALE_COOKIE: &str = "werrss_locale";

/// The locales currently supported by the administrator panel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Locale {
    /// English, the fallback locale.
    #[default]
    English,
    /// French.
    French,
    /// Simplified Chinese.
    Chinese,
}

impl Locale {
    /// Returns the stable cookie/select value for this locale.
    pub const fn code(self) -> &'static str {
        match self {
            Self::English => "en",
            Self::French => "fr",
            Self::Chinese => "zh",
        }
    }

    /// Parses a locale or language tag.
    ///
    /// `ch` is accepted as a compatibility alias because it is a common
    /// shorthand in operator configuration, although `zh` is the standard
    /// language subtag used by the panel.
    pub fn parse(value: &str) -> Option<Self> {
        let language = value
            .trim()
            .split(['-', '_'])
            .next()
            .map(str::to_ascii_lowercase)?;

        match language.as_str() {
            "en" => Some(Self::English),
            "fr" => Some(Self::French),
            "ch" | "zh" => Some(Self::Chinese),
            _ => None,
        }
    }
}

/// Selects the panel locale from a preference cookie and then language
/// negotiation headers.
pub fn from_headers(headers: &HeaderMap) -> Locale {
    let cookie_locale = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(cookie_locale);
    if let Some(locale) = cookie_locale.and_then(Locale::parse) {
        return locale;
    }

    headers
        .get(header::ACCEPT_LANGUAGE)
        .and_then(|value| value.to_str().ok())
        .and_then(accept_language)
        .unwrap_or_default()
}

fn cookie_locale(cookie: &str) -> Option<&str> {
    cookie.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name.trim() == LOCALE_COOKIE).then_some(value.trim())
    })
}

fn accept_language(value: &str) -> Option<Locale> {
    let mut candidates = Vec::new();

    for (position, item) in value.split(',').enumerate() {
        let mut parts = item.split(';');
        let language = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(language) = language else {
            continue;
        };
        let mut quality = 1_000;
        for part in parts {
            let Some((name, raw_value)) = part.trim().split_once('=') else {
                continue;
            };
            if name.trim().eq_ignore_ascii_case("q") {
                let Some(parsed) = parse_quality(raw_value) else {
                    quality = 0;
                    break;
                };
                quality = parsed;
                break;
            }
        }
        if quality == 0 {
            continue;
        }
        if let Some(locale) = Locale::parse(language) {
            candidates.push((quality, position, locale));
        }
    }

    candidates.sort_by_key(|(quality, position, _)| (Reverse(*quality), *position));
    candidates.first().map(|(_, _, locale)| *locale)
}

fn parse_quality(value: &str) -> Option<u16> {
    let value = value.trim();
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let whole = whole.parse::<u16>().ok()?;
    if whole > 1
        || fraction.len() > 3
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return None;
    }
    if whole == 1 && fraction.chars().any(|character| character != '0') {
        return None;
    }

    let mut fraction_value = fraction.parse::<u16>().unwrap_or(0);
    for _ in fraction.len()..3 {
        fraction_value *= 10;
    }
    Some(whole * 1_000 + fraction_value)
}

/// Serializes the selected locale's translation catalog for the embedded
/// browser script.
pub fn translations_json(locale: Locale) -> String {
    let catalog = TRANSLATIONS
        .iter()
        .map(|(key, english, french, chinese)| {
            let value = match locale {
                Locale::English => english,
                Locale::French => french,
                Locale::Chinese => chinese,
            };
            (*key, *value)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    serde_json::to_string(&catalog).expect("static translation catalog should serialize")
}

// Keep keys stable: they are also used by the small client-side translation
// bootstrap in `ui.rs`. Native-language names are intentionally not translated
// so the selector remains recognizable to users who switch away from their
// current language.
const TRANSLATIONS: &[(&str, &str, &str, &str)] = &[
    ("language.label", "Language", "Langue", "语言"),
    ("language.english", "English", "Anglais", "English"),
    ("language.french", "Français", "Français", "Français"),
    ("language.chinese", "中文", "中文", "中文"),
    ("app.admin_console", "Admin console", "Console d'administration", "管理控制台"),
    ("nav.signed_in_as", "Signed in as", "Connecté en tant que", "登录身份："),
    ("nav.sign_out", "Sign out", "Se déconnecter", "退出登录"),
    ("nav.dashboard", "Dashboard", "Tableau de bord", "控制面板"),
    ("nav.accounts", "Accounts", "Comptes", "账号"),
    ("nav.manage_account", "Manage account", "Gérer le compte", "管理账号"),
    ("common.breadcrumb", "Breadcrumb", "Fil d'Ariane", "面包屑导航"),
    ("login.title", "Sign in — Werrss admin", "Connexion — administration Werrss", "登录 — Werrss 管理"),
    ("login.kicker", "Private reader", "Lecteur privé", "私人阅读器"),
    ("login.heading", "Welcome back", "Bienvenue", "欢迎回来"),
    ("login.description", "Sign in to manage your sources, credentials, and feed links.", "Connectez-vous pour gérer vos sources, identifiants et liens de flux。", "登录以管理来源、凭据和订阅链接。"),
    ("login.username", "Username", "Nom d'utilisateur", "用户名"),
    ("login.password", "Password", "Mot de passe", "密码"),
    ("login.sign_in", "Sign in", "Se connecter", "登录"),
    ("login.about_aria", "About the admin console", "À propos de la console d'administration", "关于管理控制台"),
    ("login.note_heading", "A quiet place to manage your feeds.", "Un espace calme pour gérer vos flux。", "安静管理订阅的地方。"),
    ("login.note_description", "Everything here is designed for one trusted administrator.", "Tout est conçu pour un administrateur de confiance。", "这里的一切都为一位可信管理员设计。"),
    ("login.feature_controls", "Protected source and account controls", "Gestion protégée des sources et des comptes", "受保护的来源和账号管理"),
    ("login.feature_links", "Copyable RSS feed links", "Liens RSS faciles à copier", "可复制的 RSS 订阅链接"),
    ("login.feature_no_secrets", "No secrets displayed after saving", "Aucun secret n'est affiché après l'enregistrement", "保存后不会显示敏感信息"),
    ("login.error.invalid", "Sign-in failed; check the credentials or try again later.", "Échec de la connexion ; vérifiez les identifiants ou réessayez plus tard。", "登录失败；请检查凭据或稍后重试。"),
    ("login.error.unreachable", "The admin service could not be reached. Check the connection and try again.", "Le service d'administration est inaccessible。 Vérifiez la connexion et réessayez。", "无法连接管理服务。请检查连接后重试。"),
    ("dashboard.title", "Dashboard — Werrss admin", "Tableau de bord — administration Werrss", "控制面板 — Werrss 管理"),
    ("source.edit_title", "Edit source — Werrss admin", "Modifier la source — administration Werrss", "编辑来源 — Werrss 管理"),
    ("account.accounts_title", "WeRead accounts — Werrss admin", "Comptes WeRead — administration Werrss", "微信读书账号 — Werrss 管理"),
    ("account.account_title", "WeRead account — Werrss admin", "Compte WeRead — administration Werrss", "微信读书账号 — Werrss 管理"),
    ("dashboard.kicker", "Workspace overview", "Vue d'ensemble de l'espace de travail", "工作区概览"),
    ("dashboard.heading", "Good to see you.", "Ravi de vous revoir。", "很高兴再次见到你。"),
    ("dashboard.description", "Keep your feeds healthy, credentials current, and delivery links close at hand.", "Gardez vos flux fonctionnels, vos identifiants à jour et vos liens accessibles。", "保持订阅正常、凭据有效，并随时掌握投递链接。"),
    ("dashboard.add_source", "Add source", "Ajouter une source", "添加来源"),
    ("dashboard.manage_accounts", "Manage accounts", "Gérer les comptes", "管理账号"),
    ("config.heading", "Configuration backup", "Sauvegarde de la configuration", "配置备份"),
    ("config.description", "Export or preview configuration before applying it.", "Exportez ou prévisualisez la configuration avant de l'appliquer。", "导出配置，或在应用前预览更改。"),
    ("config.export_sections", "Export sections", "Sections à exporter", "导出内容"),
    ("config.sources", "Sources", "Sources", "来源"),
    ("config.accounts", "WeRead account settings", "Paramètres des comptes WeRead", "微信读书账号设置"),
    ("config.export", "Download JSON", "Télécharger le JSON", "下载 JSON"),
    ("config.import_heading", "Import configuration", "Importer la configuration", "导入配置"),
    ("config.choose_file", "Choose a configuration JSON file", "Choisir un fichier JSON de configuration", "选择配置 JSON 文件"),
    ("config.no_file", "Choose a configuration file to preview.", "Choisissez un fichier de configuration à prévisualiser。", "请选择要预览的配置文件。"),
    ("config.preview", "Preview import", "Prévisualiser l'importation", "预览导入"),
    ("config.apply", "Apply import", "Appliquer l'importation", "应用导入"),
    ("config.credential_notice", "Account settings are exported without credentials. Imported accounts need credential setup before use.", "Les paramètres des comptes sont exportés sans identifiants. Les comptes importés doivent être configurés avant utilisation。", "导出的账号设置不包含凭据。导入的账号需要先设置凭据才能使用。"),
    ("config.unsupported", "Articles, synchronization history, and cached assets are not included.", "Les articles, l'historique de synchronisation et les ressources en cache ne sont pas inclus。", "不会包含文章、同步历史记录和缓存资源。"),
    ("config.preview_ready", "Preview is ready to apply.", "L'aperçu peut être appliqué。", "预览已就绪，可以应用。"),
    ("config.preview_conflicts", "Resolve the listed conflicts before applying.", "Résolvez les conflits affichés avant l'application。", "请先解决列出的冲突，再应用导入。"),
    ("config.file_too_large", "The selected file exceeds the 2 MiB limit.", "Le fichier sélectionné dépasse la limite de 2 Mio。", "所选文件超过 2 MiB 限制。"),
    ("config.export_selection_required", "Select at least one section to export.", "Sélectionnez au moins une section à exporter。", "请至少选择一项导出内容。"),
    ("config.export_failed", "Configuration could not be exported.", "Impossible d'exporter la configuration。", "无法导出配置。"),
    ("config.preview_failed", "Configuration could not be previewed.", "Impossible de prévisualiser la configuration。", "无法预览配置。"),
    ("config.apply_failed", "Configuration could not be applied.", "Impossible d'appliquer la configuration。", "无法应用配置。"),
    ("config.result_heading", "Import result", "Résultat de l'importation", "导入结果"),
    ("config.applied", "Configuration applied.", "Configuration appliquée。", "配置已应用。"),
    ("config.conflicts_heading", "Conflicts", "Conflits", "冲突"),
    ("config.sources_created", "Sources created", "Sources créées", "新建来源"),
    ("config.sources_updated", "Sources updated", "Sources mises à jour", "更新来源"),
    ("config.accounts_created", "Accounts created", "Comptes créés", "新建账号"),
    ("config.accounts_updated", "Accounts updated", "Comptes mis à jour", "更新账号"),
    ("config.placeholders_created", "Setup-required accounts created", "Comptes à configurer créés", "新建待设置账号"),
    ("config.bindings_preserved", "Account bindings preserved", "Associations de comptes conservées", "保留账号绑定"),
    ("config.bindings_cleared", "Account bindings cleared", "Associations de comptes supprimées", "清除账号绑定"),
    ("account.setup_required_manage_action", "Set up credentials", "Configurer les identifiants", "设置凭据"),
    ("action.add_account", "Add account", "Ajouter un compte", "添加账号"),
    ("dashboard.summary_aria", "Workspace summary", "Résumé de l'espace de travail", "工作区摘要"),
    ("stats.sources", "Sources", "Sources", "来源"),
    ("stats.active_sources", "Active sources", "Sources actives", "启用的来源"),
    ("stats.weread_accounts", "WeRead accounts", "Comptes WeRead", "微信读书账号"),
    ("account.connect_heading", "Connect WeRead", "Connecter WeRead", "连接微信读书"),
    ("account.connect_description", "Enroll a browser session for authenticated source sync.", "Enregistrez une session de navigateur pour synchroniser les sources authentifiées。", "注册浏览器会话以同步需要认证的来源。"),
    ("account.cookies_notice", "Cookies are encrypted before storage and are never shown again. You can replace them later from the account page.", "Les cookies sont chiffrés avant stockage et ne sont plus jamais affichés。 Vous pourrez les remplacer depuis la page du compte。", "Cookie 会在存储前加密，保存后不会再次显示。你可以在账号页面替换它们。"),
    ("account.qr_heading", "Sign in with QR code", "Se connecter avec un code QR", "使用二维码登录"),
    ("account.qr_description", "Scan a fresh WeRead code to enroll an account without copying cookies.", "Scannez un nouveau code WeRead pour enregistrer un compte sans copier de cookies。", "扫描新的微信读书二维码，无需复制 Cookie 即可注册账号。"),
    ("account.qr_notice", "The QR code is short-lived and shown only while this page is open.", "Le code QR est temporaire et affiché uniquement pendant que cette page est ouverte。", "二维码有效期很短，仅在此页面打开时显示。"),
    ("account.id_hint_qr", "Optional; leave empty for a new account", "Facultatif ; laissez vide pour un nouveau compte", "可选；留空以创建新账号"),
    ("account.id_placeholder_qr", "Optional existing account UUID", "UUID d'un compte existant (facultatif)", "可选的已有账号 UUID"),
    ("account.display_name_placeholder_qr", "Optional override", "Remplacement facultatif", "可选覆盖名称"),
    ("account.start_qr", "Start QR login", "Démarrer la connexion QR", "开始二维码登录"),
    ("account.cancel_qr", "Cancel QR login", "Annuler la connexion QR", "取消二维码登录"),
    ("account.qr_image_alt", "WeRead login QR code", "Code QR de connexion WeRead", "微信读书登录二维码"),
    ("account.qr_waiting_for_scan", "Waiting for you to scan the QR code…", "En attente du scan du code QR…", "等待扫描二维码…"),
    ("account.qr_scanned", "Scanned; confirm the login on your phone…", "Code scanné ; confirmez la connexion sur votre téléphone…", "已扫描；请在手机上确认登录…"),
    ("account.qr_expired", "This QR login has expired. Start a new one.", "Cette connexion QR a expiré。 Démarrez-en une nouvelle。", "二维码登录已过期，请重新开始。"),
    ("account.qr_risk_controlled", "WeRead requires another login attempt. Try again later.", "WeRead exige une nouvelle tentative de connexion。 Réessayez plus tard。", "微信读书要求重新登录，请稍后再试。"),
    ("account.qr_poll_failed", "QR login status could not be checked.", "Impossible de vérifier l'état de la connexion QR。", "无法检查二维码登录状态。"),
    ("account.qr_start_failed", "QR login could not be started.", "Impossible de démarrer la connexion QR。", "无法开始二维码登录。"),
    ("account.qr_cancelled", "QR login cancelled.", "Connexion QR annulée。", "二维码登录已取消。"),
    ("account.qr_cancel_failed", "QR login could not be cancelled.", "Impossible d'annuler la connexion QR。", "无法取消二维码登录。"),
    ("account.id", "Account ID", "ID du compte", "账号 ID"),
    ("account.id_hint_new", "Optional for a new account", "Facultatif pour un nouveau compte", "新账号可选"),
    ("account.id_placeholder", "Leave blank to create an ID", "Laissez vide pour créer un ID", "留空以自动创建 ID"),
    ("account.display_name", "Display name", "Nom affiché", "显示名称"),
    ("account.display_name_hint", "Optional when wr_name is present", "Facultatif si wr_name est présent", "存在 wr_name 时可选"),
    ("account.display_name_placeholder", "e.g. Personal account", "ex. Compte personnel", "例如：个人账号"),
    ("account.cookie_header", "WeRead Cookie header", "En-tête Cookie WeRead", "微信读书 Cookie 请求头"),
    ("account.cookie_placeholder", "wr_vid=…; wr_skey=…; wr_rt=…", "wr_vid=…; wr_skey=…; wr_rt=…", "wr_vid=…; wr_skey=…; wr_rt=…"),
    ("account.access_expiry", "Access token expiry", "Expiration du jeton d'accès", "访问令牌过期时间"),
    ("account.save", "Save account", "Enregistrer le compte", "保存账号"),
    ("source.add_heading", "Add a source", "Ajouter une source", "添加来源"),
    ("source.add_description", "Start with a Book ID or resolve one from an article URL.", "Commencez par un Book ID ou résolvez-le depuis une URL d'article。", "从 Book ID 开始，或通过文章 URL 解析。"),
    ("source.unbound_notice", "Leave the account ID blank to let each sync choose a random enabled account.", "Laissez l'ID du compte vide pour choisir aléatoirement un compte activé à chaque synchronisation。", "账号 ID 留空后，每次同步会随机选择一个启用的账号。"),
    ("source.book_id", "Book ID", "Book ID", "Book ID"),
    ("source.book_id_hint", "Optional with an article URL", "Facultatif avec une URL d'article", "提供文章 URL 时可选"),
    ("source.book_id_placeholder", "e.g. MP_WXS_2103095721", "ex. MP_WXS_2103095721", "例如：MP_WXS_2103095721"),
    ("source.name", "Name", "Nom", "名称"),
    ("source.name_placeholder", "Defaults to the resolved account name", "Le nom du compte résolu par défaut", "默认使用解析出的账号名称"),
    ("source.article_url", "Article URL", "URL de l'article", "文章 URL"),
    ("source.article_url_placeholder", "https://mp.weixin.qq.com/s/…", "https://mp.weixin.qq.com/s/…", "https://mp.weixin.qq.com/s/…"),
    ("source.account_id", "WeRead account ID", "ID du compte WeRead", "微信读书账号 ID"),
    ("source.account_id_hint", "Optional", "Facultatif", "可选"),
    ("source.account_id_placeholder", "Pin this source to one account", "Associer cette source à un compte", "将此来源固定到一个账号"),
    ("source.add", "Add source", "Ajouter la source", "添加来源"),
    ("source.list_heading", "Sources", "Sources", "来源"),
    ("source.list_description", "Monitor scheduling gates and create public feed links.", "Surveillez les verrous de planification et créez des liens de flux publics。", "监控调度状态并创建公开订阅链接。"),
    ("source.new", "New source", "Nouvelle source", "新来源"),
    ("account.list_heading", "WeRead accounts", "Comptes WeRead", "微信读书账号"),
    ("account.list_description", "Enabled accounts are available to unbound source-sync jobs.", "Les comptes activés sont disponibles pour les synchronisations sans compte fixe。", "启用的账号可供未绑定账号的同步任务使用。"),
    ("account.view_all", "View all", "Tout afficher", "查看全部"),
    ("state.loading_sources", "Loading sources…", "Chargement des sources…", "正在加载来源…"),
    ("state.no_sources", "No sources yet.", "Aucune source pour le moment。", "还没有来源。"),
    ("state.first_feed", "Your first feed is one step away.", "Votre premier flux est à portée de main。", "距离你的第一个订阅只差一步。"),
    ("state.sources_load_failed", "Sources could not be loaded. Refresh and try again.", "Impossible de charger les sources。 Actualisez et réessayez。", "无法加载来源。请刷新后重试。"),
    ("state.load_sources_error", "Unable to load sources.", "Impossible de charger les sources。", "无法加载来源。"),
    ("state.loading_accounts", "Loading accounts…", "Chargement des comptes…", "正在加载账号…"),
    ("state.no_accounts", "No WeRead accounts yet.", "Aucun compte WeRead pour le moment。", "还没有微信读书账号。"),
    ("state.add_account_hint", "Add an account to enable authenticated sync.", "Ajoutez un compte pour activer la synchronisation authentifiée。", "添加账号以启用认证同步。"),
    ("state.accounts_load_failed", "Accounts could not be loaded. Refresh and try again.", "Impossible de charger les comptes。 Actualisez et réessayez。", "无法加载账号。请刷新后重试。"),
    ("state.load_accounts_error", "Unable to load WeRead accounts.", "Impossible de charger les comptes WeRead。", "无法加载微信读书账号。"),
    ("status.enabled", "Enabled", "Activée", "已启用"),
    ("status.paused", "Paused", "En pause", "已暂停"),
    ("status.active", "Active", "Actif", "活跃"),
    ("status.setup_required", "Setup required", "Configuration requise", "需要设置"),
    ("status.disabled", "Disabled", "Désactivé", "已停用"),
    ("status.warning", "Warning", "Avertissement", "警告"),
    ("status.expired", "Expired", "Expiré", "已过期"),
    ("status.blocked", "Blocked", "Bloqué", "已阻止"),
    ("status.error", "Error", "Erreur", "错误"),
    ("status.ready", "Ready", "Prêt", "就绪"),
    ("status.running", "Running", "En cours", "运行中"),
    ("status.succeeded", "Succeeded", "Réussi", "成功"),
    ("status.deferred", "Deferred", "Différé", "已延期"),
    (
        "status.authentication_required",
        "Authentication required",
        "Authentification requise",
        "需要认证",
    ),
    (
        "status.risk_controlled",
        "Risk controlled",
        "Contrôle de risque",
        "风险控制",
    ),
    (
        "status.retryable_failure",
        "Retryable failure",
        "Échec réessayable",
        "可重试失败",
    ),
    ("status.failed", "Failed", "Échec", "失败"),
    ("source.scheduling", "Scheduling", "Planification", "调度"),
    ("source.edit", "Edit", "Modifier", "编辑"),
    ("source.gate_ready", "Gate ready", "Verrou prêt", "状态已就绪"),
    ("source.clear_gate", "Clear gate", "Effacer le verrou", "清除状态"),
    ("source.create_feed_link", "Create feed link", "Créer un lien de flux", "创建订阅链接"),
    ("source.history", "Show sync history", "Afficher l'historique des synchronisations", "显示同步历史"),
    ("source.history_loading", "Loading history…", "Chargement de l'historique…", "正在加载历史…"),
    ("source.no_history", "No synchronization runs yet.", "Aucune synchronisation pour le moment。", "还没有同步记录。"),
    ("source.history_unavailable", "History is temporarily unavailable.", "L'historique est temporairement indisponible。", "历史记录暂时不可用。"),
    ("account.current_status", "Current status", "Statut actuel", "当前状态"),
    ("account.manage", "Manage", "Gérer", "管理"),
    ("account.manage_account", "Manage account", "Gérer le compte", "管理账号"),
    ("source.updated", "Source updated.", "Source mise à jour。", "来源已更新。"),
    ("source.update_failed", "Source could not be updated.", "Impossible de mettre à jour la source。", "无法更新来源。"),
    ("source.status_change_failed", "Source status could not be changed.", "Impossible de modifier le statut de la source。", "无法更改来源状态。"),
    ("source.gate_clear_failed", "Source gate could not be cleared.", "Impossible d'effacer le verrou de la source。", "无法清除来源状态。"),
    ("source.delete_confirm", "Delete this source and its stored articles permanently?", "Supprimer définitivement cette source et ses articles stockés ?", "永久删除此来源及其已存储文章？"),
    ("source.delete_failed", "Source could not be deleted.", "Impossible de supprimer la source。", "无法删除来源。"),
    ("account.rotation_notice", "For security, the stored cookie is never displayed. Paste a complete fresh Cookie request-header value when rotating credentials.", "Pour votre sécurité, le cookie stocké n'est jamais affiché。 Collez une valeur complète et récente de l'en-tête Cookie lors de la rotation。", "出于安全考虑，存储的 Cookie 永不显示。更新凭据时请粘贴完整且最新的 Cookie 请求头。"),
    ("account.new_cookie_header", "New WeRead Cookie header", "Nouvel en-tête Cookie WeRead", "新的微信读书 Cookie 请求头"),
    ("account.display_name_help_before", "You may leave this blank if the cookie contains a usable ", "Vous pouvez laisser ce champ vide si le cookie contient un ", "如果 Cookie 包含可用的 "),
    ("account.display_name_help_after", ".", " utilisable.", "，可以留空。"),
    ("account.account_status", "Account status", "Statut du compte", "账号状态"),
    ("account.disabled_help", "Disabled accounts are skipped by random selection.", "Les comptes désactivés sont ignorés lors de la sélection aléatoire。", "随机选择时会跳过已停用的账号。"),
    ("account.updated", "Account updated.", "Compte mis à jour。", "账号已更新。"),
    ("account.update_failed", "Account could not be updated.", "Impossible de mettre à jour le compte。", "无法更新账号。"),
    ("account.status_change_failed", "Account status could not be changed.", "Impossible de modifier le statut du compte。", "无法更改账号状态。"),
    ("account.delete_confirm", "Delete this WeRead account permanently?", "Supprimer définitivement ce compte WeRead ?", "永久删除此微信读书账号？"),
    ("account.delete_failed", "Account could not be deleted.", "Impossible de supprimer le compte。", "无法删除账号。"),
    ("account.delete_description", "Deleting removes the stored credentials permanently.", "La suppression retire définitivement les identifiants stockés。", "删除会永久移除已存储的凭据。"),
    ("account.credentials_kicker", "Credentials", "Identifiants", "凭据"),
    ("account.credential_settings_kicker", "Credential settings", "Paramètres des identifiants", "凭据设置"),
    ("account.manage_heading", "Manage WeRead account", "Gérer le compte WeRead", "管理微信读书账号"),
    ("account.manage_description", "Rotate the browser session or update its display name.", "Renouvelez la session du navigateur ou modifiez son nom affiché。", "更新浏览器会话或显示名称。"),
    ("account.details", "Account details", "Détails du compte", "账号详情"),
    ("account.page_description", "Keep authenticated browser sessions healthy without exposing stored cookies.", "Maintenez les sessions de navigateur authentifiées sans exposer les cookies stockés。", "在不暴露已存储 Cookie 的情况下维护认证浏览器会话。"),
    ("account.directory", "Account directory", "Répertoire des comptes", "账号目录"),
    ("account.directory_description", "Active accounts can be selected for unbound source synchronization.", "Les comptes actifs peuvent être sélectionnés pour les sources sans compte fixe。", "未绑定账号的来源同步可以选择活跃账号。"),
    ("account.credentials_notice", "Credentials are encrypted at rest. Manage an account to replace its cookie header or change its status.", "Les identifiants sont chiffrés au repos。 Gérez un compte pour remplacer son en-tête Cookie ou modifier son statut。", "凭据会加密存储。进入账号管理页面可替换 Cookie 请求头或更改状态。"),
    ("account.setup_required_notice", "This account needs WeRead credentials before it can be used. Enroll a fresh cookie header below.", "Ce compte nécessite des identifiants WeRead avant utilisation. Ajoutez un nouvel en-tête Cookie ci-dessous。", "此账号需要先设置微信读书凭据才能使用。请在下方填写新的 Cookie 请求头。"),
    ("account.setup_required_manage_description", "Enroll credentials to activate this account.", "Ajoutez des identifiants pour activer ce compte。", "设置凭据以启用此账号。"),
    ("state.no_accounts_page", "No WeRead accounts have been added.", "Aucun compte WeRead n'a été ajouté。", "还没有添加微信读书账号。"),
    ("state.add_first_account", "Add your first account from the dashboard.", "Ajoutez votre premier compte depuis le tableau de bord。", "请从控制面板添加你的第一个账号。"),
    ("state.accounts_page_load_failed", "Accounts could not be loaded. Refresh and try again.", "Impossible de charger les comptes。 Actualisez et réessayez。", "无法加载账号。请刷新后重试。"),
    ("state.accounts_page_error", "Unable to load WeRead accounts.", "Impossible de charger les comptes WeRead。", "无法加载微信读书账号。"),
    ("action.cancel", "Cancel", "Annuler", "取消"),
    ("action.save_changes", "Save changes", "Enregistrer les modifications", "保存更改"),
    ("action.delete_source", "Delete source", "Supprimer la source", "删除来源"),
    ("action.delete_account", "Delete account", "Supprimer le compte", "删除账号"),
    ("action.pause", "Pause", "Mettre en pause", "暂停"),
    ("action.enable", "Enable", "Activer", "启用"),
    ("action.disable", "Disable", "Désactiver", "停用"),
    ("common.source", "source", "source", "来源"),
    ("common.account", "account", "compte", "账号"),
    ("common.unreachable", "The admin service could not be reached. Try again.", "Le service d'administration est inaccessible。 Réessayez。", "无法连接管理服务。请重试。"),
    ("common.feed_response_invalid", "The feed link response was invalid.", "La réponse du lien de flux est invalide。", "订阅链接响应无效。"),
    ("common.source_added", "Source added.", "Source ajoutée。", "来源已添加。"),
    ("common.account_saved", "Saved account", "Compte enregistré", "已保存账号"),
    ("common.account_saved_detail", "Saved account {id}; use this ID when adding a source.", "Compte {id} enregistré ; utilisez cet ID lors de l'ajout d'une source。", "账号 {id} 已保存；添加来源时请使用此 ID。"),
    ("common.account_save_failed", "WeRead account could not be saved; check the values and try again.", "Impossible d'enregistrer le compte WeRead ; vérifiez les valeurs et réessayez。", "无法保存微信读书账号；请检查填写内容后重试。"),
    ("common.source_add_failed", "Source could not be added.", "Impossible d'ajouter la source。", "无法添加来源。"),
    ("common.feed_link_failed", "Unable to create a feed link.", "Impossible de créer un lien de flux。", "无法创建订阅链接。"),
    ("common.source_status_failed", "Unable to change source status.", "Impossible de modifier le statut de la source。", "无法更改来源状态。"),
    ("common.source_gate_failed", "Unable to clear source gate.", "Impossible d'effacer le verrou de la source。", "无法清除来源状态。"),
    ("source.settings_kicker", "Source settings", "Paramètres de la source", "来源设置"),
    ("source.edit_description", "Update how this source is identified, scheduled, and delivered.", "Modifiez l'identification, la planification et la diffusion de cette source。", "更新此来源的标识、调度和投递设置。"),
    ("source.pause_source", "Pause source", "Mettre la source en pause", "暂停来源"),
    ("source.enable_source", "Enable source", "Activer la source", "启用来源"),
    ("account.disable_account", "Disable account", "Désactiver le compte", "停用账号"),
    ("account.enable_account", "Enable account", "Activer le compte", "启用账号"),
    ("source.configuration", "Configuration", "Configuration", "配置"),
    ("source.config_description", "Changes take effect on the next synchronization cycle.", "Les changements prennent effet au prochain cycle de synchronisation。", "更改将在下一同步周期生效。"),
    ("source.feed_identity", "Feed identity", "Identité du flux", "订阅标识"),
    ("source.article_url_hint", "Optional for Book ID-only sources", "Facultatif pour les sources uniquement identifiées par Book ID", "仅使用 Book ID 的来源可选"),
    ("source.article_url_help", "Clear it when the source is identified only by Book ID.", "Effacez-le si la source est identifiée uniquement par son Book ID。", "如果来源仅通过 Book ID 标识，请清空此项。"),
    ("source.account_id_help", "Clear it to let the worker choose an enabled account.", "Effacez-le pour laisser le worker choisir un compte activé。", "清空后由 worker 选择一个启用的账号。"),
    ("source.delivery_policy", "Delivery policy", "Politique de diffusion", "投递策略"),
    ("source.sync_interval", "Sync interval (seconds)", "Intervalle de synchronisation (secondes)", "同步间隔（秒）"),
    ("source.rss_item_limit", "RSS item limit", "Limite d'éléments RSS", "RSS 条目上限"),
    ("source.priority", "Priority", "Priorité", "优先级"),
    ("source.maximum_attempts", "Maximum attempts", "Nombre maximal de tentatives", "最大尝试次数"),
    ("source.runtime_status", "Runtime status", "État d'exécution", "运行状态"),
    ("source.runtime_status_description", "Current scheduling state.", "État actuel de la planification。", "当前调度状态。"),
    ("source.status", "Status", "Statut", "状态"),
    ("source.gate", "Gate", "Verrou", "状态门"),
    ("source.revision", "Revision", "Révision", "版本"),
    ("source.danger_zone", "Danger zone", "Zone dangereuse", "危险区域"),
    ("source.delete_description", "Deleting removes this source and its stored articles.", "La suppression retire cette source et ses articles stockés。", "删除会移除此来源及其已存储文章。"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locale_cookie_takes_precedence_over_browser_language() {
        let headers = HeaderMap::from_iter([
            (header::COOKIE, "werrss_locale=fr".parse().unwrap()),
            (header::ACCEPT_LANGUAGE, "zh-CN, en;q=0.8".parse().unwrap()),
        ]);

        assert_eq!(from_headers(&headers), Locale::French);
    }

    #[test]
    fn accept_language_selects_highest_quality_supported_locale() {
        let headers = HeaderMap::from_iter([(
            header::ACCEPT_LANGUAGE,
            "de, fr-FR;q=0.8, zh-CN;q=0.9".parse().unwrap(),
        )]);

        assert_eq!(from_headers(&headers), Locale::Chinese);
    }

    #[test]
    fn unsupported_or_zero_quality_languages_fall_back_to_english() {
        let zero_quality =
            HeaderMap::from_iter([(header::ACCEPT_LANGUAGE, "fr;q=0, de;q=0.5".parse().unwrap())]);
        assert_eq!(from_headers(&zero_quality), Locale::English);

        let unsupported =
            HeaderMap::from_iter([(header::ACCEPT_LANGUAGE, "ja-JP, ko-KR".parse().unwrap())]);
        assert_eq!(from_headers(&unsupported), Locale::English);
    }

    #[test]
    fn malformed_quality_values_do_not_outrank_valid_language_preferences() {
        let headers = HeaderMap::from_iter([(
            header::ACCEPT_LANGUAGE,
            "fr;q=not-a-number, zh-CN;Q=0.5".parse().unwrap(),
        )]);

        assert_eq!(from_headers(&headers), Locale::Chinese);
    }

    #[test]
    fn chinese_shorthand_and_region_tags_are_accepted() {
        assert_eq!(Locale::parse("ch"), Some(Locale::Chinese));
        assert_eq!(Locale::parse("zh-CN"), Some(Locale::Chinese));
        assert_eq!(Locale::parse("fr_CA"), Some(Locale::French));
    }

    #[test]
    fn translation_catalog_contains_distinct_french_and_chinese_values() {
        let french: serde_json::Value =
            serde_json::from_str(&translations_json(Locale::French)).unwrap();
        let chinese: serde_json::Value =
            serde_json::from_str(&translations_json(Locale::Chinese)).unwrap();

        assert_eq!(french["dashboard.heading"], "Ravi de vous revoir。");
        assert_eq!(chinese["dashboard.heading"], "很高兴再次见到你。");
    }

    #[test]
    fn configuration_transfer_and_setup_required_copy_exists_in_all_locales() {
        for locale in [Locale::English, Locale::French, Locale::Chinese] {
            let translations: serde_json::Value =
                serde_json::from_str(&translations_json(locale)).unwrap();

            for key in [
                "config.heading",
                "config.description",
                "config.export_sections",
                "config.sources",
                "config.accounts",
                "config.export",
                "config.import_heading",
                "config.choose_file",
                "config.no_file",
                "config.preview",
                "config.apply",
                "config.unsupported",
                "config.preview_ready",
                "config.preview_conflicts",
                "config.file_too_large",
                "config.export_selection_required",
                "config.export_failed",
                "config.preview_failed",
                "config.apply_failed",
                "config.credential_notice",
                "config.result_heading",
                "config.applied",
                "config.conflicts_heading",
                "config.sources_created",
                "config.sources_updated",
                "config.accounts_created",
                "config.accounts_updated",
                "config.placeholders_created",
                "config.bindings_preserved",
                "config.bindings_cleared",
                "account.setup_required_manage_action",
            ] {
                assert!(
                    !translations[key].as_str().unwrap_or_default().is_empty(),
                    "missing {key} in {}",
                    locale.code()
                );
            }
        }
    }
}
