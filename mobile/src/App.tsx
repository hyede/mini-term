import { selectDesktop, useRelayStore } from './relay';
import { useI18nStore, useT } from './i18n';
import { SessionList } from './SessionList';
import { MirrorView } from './MirrorView';

function LanguageToggle() {
  const lang = useI18nStore((s) => s.lang);
  const setLang = useI18nStore((s) => s.setLang);
  return (
    <div className="lang-toggle">
      <button className={lang === 'zh' ? 'active' : ''} onClick={() => setLang('zh')}>中文</button>
      <button className={lang === 'en' ? 'active' : ''} onClick={() => setLang('en')}>EN</button>
    </div>
  );
}

function StatusScreen({ icon, title, hint, spinning }: {
  icon: string;
  title: string;
  hint?: string;
  spinning?: boolean;
}) {
  return (
    <div className="status-screen">
      <div className={`status-icon ${spinning ? 'spinning' : ''}`}>{icon}</div>
      <div className="status-title">{title}</div>
      {hint && <div className="status-hint">{hint}</div>}
    </div>
  );
}

export function App() {
  const t = useT();
  const phase = useRelayStore((s) => s.phase);
  const rejectReason = useRelayStore((s) => s.rejectReason);
  const mirrorOpen = useRelayStore((s) => s.mirror !== null);
  const devices = useRelayStore((s) => s.devices);
  const selectedDesktopId = useRelayStore((s) => s.selectedDesktopId);
  const switchingDisabled = useRelayStore((s) => !!s.mirror?.pendingCommandId || !!s.starting);

  let body;
  switch (phase) {
    case 'idle':
      body = <StatusScreen icon="▣" title={t('pair.appName')} hint={t('pair.scanHint')} />;
      break;
    case 'pairing':
      body = <StatusScreen icon="◌" title={t('pair.pairing')} spinning />;
      break;
    case 'connecting':
      body = <StatusScreen icon="◌" title={t('pair.connecting')} spinning />;
      break;
    case 'reconnecting':
      body = <StatusScreen icon="◌" title={t('pair.reconnecting')} spinning />;
      break;
    case 'connected':
      body = mirrorOpen ? <MirrorView /> : <SessionList />;
      break;
    case 'revoked':
      body = <StatusScreen icon="⊘" title={t('pair.revoked')} hint={t('pair.revokedHint')} />;
      break;
    case 'rejected':
      body = (
        <StatusScreen
          icon="⊘"
          title={t(`pair.rejected.${rejectReason ?? 'missingAuth'}`)}
        />
      );
      break;
  }

  return (
    <div className="app">
      <header className="app-header">
        <span className="app-name">{t('pair.appName')}</span>
        {phase === 'connected' && devices.length > 0 && (
          <label className="desktop-picker">
            <span>{t('pair.desktop')}</span>
            <select value={selectedDesktopId ?? ''} disabled={switchingDisabled}
              onChange={(event) => selectDesktop(event.target.value)}>
              {devices.map((device) => (
                <option key={device.desktopId} value={device.desktopId}>
                  {device.name}{device.online ? '' : ' · ' + t('pair.offline')}
                </option>
              ))}
            </select>
          </label>
        )}
        <LanguageToggle />
      </header>
      <main className={`app-main ${phase === 'connected' ? 'list-mode' : ''}`}>{body}</main>
    </div>
  );
}
