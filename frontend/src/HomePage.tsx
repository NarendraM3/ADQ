import { BrainCircuit, ChevronDown, LineChart, ShieldCheck, Check } from 'lucide-react';
import './home.css';

const features = [
  { icon: LineChart, title: 'Automated Quality', body: 'AI-powered rules and anomaly detection ensure data quality at scale.' },
  { icon: ShieldCheck, title: 'Trusted Data', body: 'Continuous validation across sources and pipelines you can rely on.' },
  { icon: BrainCircuit, title: 'Intelligent Decisions', body: 'High-quality data fuels AI, analytics, and confident business outcomes.' },
];
const links = ['Product', 'Solutions', 'Resources', 'Company'];

export function HomePage() {
  return <main className="home-page" id="home">
    <div className="home-ambient" aria-hidden="true"><div className="ambient-orb orb-a"/><div className="ambient-orb orb-b"/><SideDataArt/></div>
    <header className="site-nav">
      <a className="nav-brand" href="#home" aria-label="ADQ Automated Data Quality home">
        <ShieldCheck className="nav-shield" size={27} strokeWidth={1.8}/>
        <span className="nav-brand-copy"><strong>ADQ</strong><small>Automated Data Quality</small></span>
      </a>
      <nav aria-label="Main navigation">{links.map(link=><a href={`#${link.toLowerCase()}`} key={link}>{link}<ChevronDown size={14}/></a>)}</nav>
      <a className="nav-contact" href="mailto:contact@adq.ai">Contact Us</a>
    </header>
    <section className="home-hero" aria-labelledby="hero-title">
      <div className="hero-content">
        <div className="hero-mark" aria-label="ADQ"><span>A</span><span>D</span><span className="hero-q">Q<Check size={40} strokeWidth={3}/></span></div>
        <h1 id="hero-title">AI-driven quality. Trusted data.<br/><span>Intelligent decisions.</span></h1>
        <p className="hero-description">ADQ automates data quality at every layer—so you can trust your data, accelerate insights, and drive confident decisions.</p>
        <a className="hero-cta" href="/login">Sign in to continue</a>
      </div>
      <div className="hero-cards">{features.map(({icon: Icon,title,body})=><article className="hero-card" key={title}><span className="card-icon"><Icon size={25} strokeWidth={1.7}/></span><h2>{title}</h2><p>{body}</p></article>)}</div>
    </section>
  </main>;
}

function SideDataArt() {
  return <svg className="side-data-art" viewBox="0 0 1400 720" fill="none" aria-hidden="true">
    <defs><linearGradient id="side-wave" x1="0" y1="0" x2="1" y2="1"><stop stopColor="#68E2CE" stopOpacity=".02"/><stop offset=".55" stopColor="#68E2CE" stopOpacity=".32"/><stop offset="1" stopColor="#68E2CE" stopOpacity=".04"/></linearGradient><filter id="icon-glow" x="-80%" y="-80%" width="260%" height="260%"><feGaussianBlur stdDeviation="5" result="blur"/><feComposite in="SourceGraphic" in2="blur" operator="over"/></filter></defs>
    <g className="side-waves" stroke="url(#side-wave)" strokeWidth="1.2"><path d="M-30 310C110 225 180 397 315 305S495 222 570 275"/><path d="M-30 328C110 243 180 415 315 323S495 240 570 293"/><path d="M830 390C930 320 1015 458 1110 372S1280 300 1440 360"/><path d="M830 408C930 338 1015 476 1110 390S1280 318 1440 378"/></g>
    <g className="side-objects" stroke="#68E2CE" strokeOpacity=".30" strokeWidth="1.25">
      <g transform="translate(150 405)"><ellipse cx="0" cy="0" rx="37" ry="11"/><path d="M-37 0v51c0 6 17 11 37 11s37-5 37-11V0M-37 25c0 6 17 11 37 11s37-5 37-11"/><ellipse cy="0" rx="37" ry="11"/></g>
      <g transform="translate(1150 240)"><ellipse cx="0" cy="0" rx="34" ry="10"/><path d="M-34 0v45c0 6 15 10 34 10s34-4 34-10V0M-34 22c0 6 15 10 34 10s34-4 34-10"/><ellipse cy="0" rx="34" ry="10"/></g>
      <path d="M260 170l27 10v22c0 20-13 33-27 40-14-7-27-20-27-40v-22l27-10z"/><path d="m248 202 8 8 16-18"/><path d="M1040 450l25 9v21c0 19-12 31-25 38-13-7-25-19-25-38v-21l25-9z"/><path d="m1029 479 7 7 15-17"/>
      <path d="M189 405l71-204m0 0 49 52m-49-52 62-52M1150 240l-86 35m86-35 60 61m-60-61-54-112m-116 322-91-73"/>
    </g>
    <g className="side-icon-detail" stroke="#71f5e9" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" filter="url(#icon-glow)">
      {/* Left data store, linked to a small validation shield */}
      <g transform="translate(150 405)"><path d="M-21-4v32c0 4 9 8 21 8s21-4 21-8V-4"/><ellipse rx="21" ry="7"/><path d="M-21 12c0 4 9 8 21 8s21-4 21-8M-38-8l12-9m-12 9v12m50-21 13-8m-13 8v12M-38 31l12 8m26-8 13 8"/><circle cx="-38" cy="-8" r="2" fill="#71f5e9"/><circle cx="-38" cy="4" r="2" fill="#71f5e9"/><circle cx="26" cy="-17" r="2" fill="#71f5e9"/><circle cx="26" cy="39" r="2" fill="#71f5e9"/></g>
      <g transform="translate(260 201) scale(.62)"><path d="M0-24 29-13v25c0 21-14 36-29 44-15-8-29-23-29-44v-25L0-24z"/><path d="m-13 4 9 9L15-8"/></g>
      {/* Larger paired trust and storage symbols on the right */}
      <g transform="translate(1040 450) scale(1.18)"><path d="M0-25 30-14v27c0 22-14 37-30 46-16-9-30-24-30-46v-27L0-25z"/><path d="m-14 4 10 10L16-9"/></g>
      <g transform="translate(1150 240) scale(1.12)"><path d="M-28-3v40c0 6 13 11 28 11s28-5 28-11V-3"/><ellipse rx="28" ry="9"/><path d="M-28 17c0 5 13 10 28 10s28-5 28-10"/></g>
    </g>
    <g className="side-nodes" fill="#68E2CE"><circle cx="260" cy="201" r="3"/><circle cx="309" cy="253" r="2.5"/><circle cx="371" cy="149" r="2.5"/><circle cx="1064" cy="275" r="3"/><circle cx="1210" cy="301" r="2.5"/><circle cx="1096" cy="128" r="2.5"/><circle cx="150" cy="405" r="3"/><circle cx="1150" cy="240" r="3"/></g>
    <g className="network-extra" stroke="#68E2CE" strokeOpacity=".25"><path d="m1020 168 54-40 40 48 66-20 30 58M1074 128l28-53m12 101 50 48m-50-48 16-72m-6 120 62 0"/><circle cx="1020" cy="168" r="5"/><circle cx="1074" cy="128" r="5"/><circle cx="1114" cy="176" r="5"/><circle cx="1180" cy="156" r="5"/><circle cx="1210" cy="214" r="5"/></g>
  </svg>;
}
