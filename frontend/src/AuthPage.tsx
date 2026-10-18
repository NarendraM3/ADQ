import { useState } from 'react';
import { Check, CheckCircle2, Cloud, Database, Eye, EyeOff, Grid2X2, LockKeyhole, Mail, ShieldCheck, Table2 } from 'lucide-react';
import './auth.css';

type Mode = 'login' | 'register';

export function AuthPage() {
  const [mode, setMode] = useState<Mode>('login');
  const [showPassword, setShowPassword] = useState(false);
  const [notice, setNotice] = useState('');
  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (mode === 'login') {
      window.location.assign('/onboarding');
      return;
    }
    setNotice('Account registration is unavailable: this application has no authentication endpoint configured.');
  };

  return <main className="auth-page">
    <div className="auth-glow auth-glow-one"/><div className="auth-glow auth-glow-two"/>
    <div className="auth-shell">
      <header className="auth-brand" aria-label="ADQ Automated Data Quality">
        <span className="auth-logo"><svg viewBox="0 0 44 44" aria-hidden="true"><path d="M22 3.5 38 12.75v18.5L22 40.5 6 31.25v-18.5L22 3.5Z"/><path d="m22 11 9.5 5.5v11L22 33l-9.5-5.5v-11L22 11Z"/><path d="m22 18 3.5 2v4L22 26l-3.5-2v-4l3.5-2Z"/></svg></span>
        <span className="auth-brand-name"><span className="brand-aether">A</span><span className="brand-dq">DQ</span></span><span className="auth-brand-subtitle">Automated Data Quality</span>
      </header>

      <section className="auth-card" aria-label="Sign in to ADQ">
        <aside className="auth-flow">
          <div className="flow-intro"><span className="flow-kicker"><i/> DATA CONFIDENCE, BUILT IN</span><h1>Clean data.<br/><span>Clear decisions.</span></h1><p>One reliable flow from raw sources to trusted destinations.</p></div>
          <div className="flow-list">
            <div className="flow-line" aria-hidden="true"/>
            <FlowStep number="01" title="DATA SOURCES" detail="Connect your data" icon={<><Database/><Cloud/><Table2/></>} />
            <FlowStep number="02" title="QUALITY CHECKS" detail="Validate every record" icon={<><ShieldCheck/><Check/></>} />
            <FlowStep number="03" title="DESTINATION" detail="Deliver with confidence" icon={<Database/>} />
          </div>
          <div className="flow-foot"><span className="flow-foot-icon"><CheckCircle2 size={15}/></span><span><b>Your data, in good hands</b><small>Secure pipelines. Trusted outcomes.</small></span><span className="flow-foot-spark">✳</span></div>
        </aside>

        <section className="auth-form-panel">
          <div className="auth-tabs" role="tablist" aria-label="Account access">
            <button role="tab" aria-selected={mode === 'login'} className={mode === 'login' ? 'active' : ''} onClick={() => { setMode('login'); setNotice(''); }}>Login</button>
            <button role="tab" aria-selected={mode === 'register'} className={mode === 'register' ? 'active' : ''} onClick={() => { setMode('register'); setNotice(''); }}>Register</button>
          </div>
          <div className="auth-heading"><span className="auth-overline">{mode === 'login' ? 'WELCOME BACK' : 'GET STARTED'}</span><h2>{mode === 'login' ? 'Welcome back!' : 'Create your account'}</h2><p>{mode === 'login' ? 'Please sign in to continue.' : 'Start building a more trusted data pipeline.'}</p></div>
          <form className="auth-form" onSubmit={submit}>
            {mode === 'register' && <label className="auth-field"><span>Full name</span><span className="auth-input"><Grid2X2 size={17}/><input autoComplete="name" placeholder="Your name" required/></span></label>}
            <label className="auth-field"><span>Email address</span><span className="auth-input"><Mail size={17}/><input type="email" autoComplete="email" placeholder="you@company.com" required/></span></label>
            <label className="auth-field"><span>Password</span><span className="auth-input"><LockKeyhole size={17}/><input type={showPassword ? 'text' : 'password'} autoComplete={mode === 'login' ? 'current-password' : 'new-password'} placeholder="Enter your password" minLength={8} required/><button className="password-toggle" type="button" aria-label={showPassword ? 'Hide password' : 'Show password'} onClick={() => setShowPassword(!showPassword)}>{showPassword ? <EyeOff size={17}/> : <Eye size={17}/>}</button></span></label>
            {mode === 'login' && <div className="auth-forgot-row"><a href="#forgot" onClick={event => { event.preventDefault(); setNotice('Password reset is unavailable: this application has no authentication endpoint configured.'); }}>Forgot password?</a></div>}
            <button className="auth-submit" type="submit">{mode === 'login' ? 'Sign In' : 'Create Account'}<span aria-hidden="true">→</span></button>
          </form>
          <div className="auth-divider"><span/><small>Or continue with</small><span/></div>
          <div className="auth-providers"><button type="button" onClick={() => setNotice('Google sign in is not configured for this application.')}><GoogleMark/><span>Google</span></button><button type="button" onClick={() => setNotice('Microsoft sign in is not configured for this application.')}><MicrosoftMark/><span>Microsoft</span></button></div>
          <p className="auth-terms">By continuing, you agree to our <a href="#terms" onClick={e=>e.preventDefault()}>Terms</a> and <a href="#privacy" onClick={e=>e.preventDefault()}>Privacy Policy</a>.</p>
          {notice && <div className="auth-notice" role="status">{notice}</div>}
        </section>
      </section>
      <footer className="auth-footer"><span>© 2026 ADQ</span><span className="auth-footer-secure"><LockKeyhole size={12}/> Secure data quality platform</span><a href="#help" onClick={e=>e.preventDefault()}>Need help?</a></footer>
    </div>
  </main>;
}

function FlowStep({ number, title, detail, icon }: { number: string; title: string; detail: string; icon: React.ReactNode }) {
  return <div className="flow-step"><div className="flow-step-icons">{icon}</div><div className="flow-step-copy"><span>{number} <i> / </i> PIPELINE</span><b>{title}</b><small>{detail}</small></div><span className="flow-check"><Check size={14}/></span></div>;
}

function GoogleMark() { return <svg className="provider-mark" viewBox="0 0 20 20" aria-hidden="true"><path fill="#4285F4" d="M19.6 10.23c0-.68-.06-1.36-.18-2H10v3.8h5.38a4.6 4.6 0 0 1-2 3.02v2.47h3.24c1.9-1.75 2.98-4.32 2.98-7.29Z"/><path fill="#34A853" d="M10 20c2.7 0 4.97-.9 6.62-2.48l-3.24-2.47c-.9.6-2.04.96-3.38.96-2.6 0-4.8-1.75-5.59-4.1H1.06v2.54A10 10 0 0 0 10 20Z"/><path fill="#FBBC05" d="M4.41 11.91a6 6 0 0 1 0-3.82V5.55H1.06a10 10 0 0 0 0 8.9l3.35-2.54Z"/><path fill="#EA4335" d="M10 3.99c1.47 0 2.79.5 3.83 1.52l2.87-2.87A9.6 9.6 0 0 0 10 0a10 10 0 0 0-8.94 5.55l3.35 2.54C5.2 5.74 7.4 3.99 10 3.99Z"/></svg>; }
function MicrosoftMark() { return <svg className="provider-mark microsoft-mark" viewBox="0 0 20 20" aria-hidden="true"><path fill="#f25022" d="M1 1h8.5v8.5H1z"/><path fill="#7fba00" d="M10.5 1H19v8.5h-8.5z"/><path fill="#00a4ef" d="M1 10.5h8.5V19H1z"/><path fill="#ffb900" d="M10.5 10.5H19V19h-8.5z"/></svg>; }
