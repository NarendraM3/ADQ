import { BrainCircuit, Database, ShieldCheck, ArrowUpRight } from 'lucide-react';
import './home.css';

const features = [
  { icon: BrainCircuit, title: 'AI Rules', body: 'Turn your quality standards into intelligent rules that learn and adapt with your data.' },
  { icon: Database, title: 'Multi-Platform', body: 'Bring data together across your stack, from everyday files to enterprise warehouses.' },
  { icon: ShieldCheck, title: 'Validate & Correct', body: 'Find issues early, review smart suggestions, and deliver data you can trust.' },
];

export function HomePage() {
  return <main className="home-page" id="home">
    <div className="home-ambient" aria-hidden="true"><div className="ambient-orb orb-a"/><div className="ambient-orb orb-b"/><NetworkArt/></div>
    <header className="site-nav"><a className="nav-brand" href="#home" aria-label="ADQ home"><span className="nav-brand-word"><span className="brand-aether">A</span><span className="brand-dq">DQ</span></span><i/></a><nav aria-label="Main navigation"><a href="#home">Home</a><a href="/login">Sign in</a></nav><a className="nav-login" href="/login">Sign in <ArrowUpRight size={14}/></a></header>
    <section className="home-hero" aria-labelledby="hero-title">
      <div className="hero-orbit orbit-one"/><div className="hero-orbit orbit-two"/>
      <div className="hero-content"><div className="hero-eyebrow"><span/> INTELLIGENCE FOR YOUR DATA <span/></div><div className="hero-mark"><span><span className="brand-aether">A</span><span className="brand-dq">DQ</span></span><i/></div><p className="hero-subtitle">AUTOMATED DATA QUALITY</p><h1 id="hero-title">AI-driven quality. <span>Trusted data.</span><br/>Intelligent decisions.</h1><p className="hero-description">Make every data point count. ADQ brings clarity, consistency, and confidence to the data that drives your business.</p><a className="hero-cta" href="/login">Sign in to continue <ArrowUpRight size={15}/></a>
        <div className="hero-proof"><span className="proof-line"/><span>QUALITY AT EVERY STEP</span><span className="proof-line"/></div>
      </div>
      <div className="hero-cards">{features.map(({icon: Icon,title,body},i)=><article className="hero-card" key={title}><div className="card-top"><span className="card-icon"><Icon size={17} strokeWidth={1.6}/></span><span className="card-index">0{i+1}</span></div><h2>{title}</h2><p>{body}</p><span className="card-bottom-line"/></article>)}</div>
    </section>
  </main>;
}

function NetworkArt() {
  const dots = [[3,38],[11,26],[16,41],[23,18],[29,32],[38,23],[45,43],[53,17],[61,32],[69,21],[77,40],[85,27],[94,35],[7,66],[20,58],[32,71],[44,61],[56,75],[68,59],[80,68],[91,57]];
  return <svg className="network-art" viewBox="0 0 1200 620" preserveAspectRatio="xMidYMid slice" aria-hidden="true"><defs><linearGradient id="netLine" x1="0" y1="0" x2="1" y2="1"><stop stopColor="#5de7d0" stopOpacity="0"/><stop offset=".5" stopColor="#5de7d0" stopOpacity=".52"/><stop offset="1" stopColor="#45b9e6" stopOpacity="0"/></linearGradient><filter id="netGlow"><feGaussianBlur stdDeviation="3" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs><g className="network-lines">{dots.map(([x,y],i)=>{const next=dots[(i+1)%dots.length];const next2=dots[(i+5)%dots.length];return <g key={i}><path d={`M ${x*12} ${y*6.2} Q ${(x+next[0])*6} ${((y+next[1])*3.1)-16} ${next[0]*12} ${next[1]*6.2}`} /><path d={`M ${x*12} ${y*6.2} Q ${(x+next2[0])*6} ${((y+next2[1])*3.1)+18} ${next2[0]*12} ${next2[1]*6.2}`} className="network-line-soft"/></g>})}</g><g filter="url(#netGlow)">{dots.map(([x,y],i)=><circle className="network-dot" key={i} cx={x*12} cy={y*6.2} r={i%4===0?2.2:1.2}/>)}</g></svg>;
}
