import React from 'react';
import { createRoot } from 'react-dom/client';
import { AuthPage } from './AuthPage';
import { HomePage } from './HomePage';
import Onboarding from './Onboarding';
import JobsPage from './JobsPage';
import './brand.css';

const isLogin = window.location.pathname === '/login';
const isOnboarding = window.location.pathname === '/onboarding';
const isJobs = window.location.pathname === '/jobs';

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>{isLogin ? <AuthPage /> : isOnboarding ? <Onboarding onSelect={() => {}} /> : isJobs ? <JobsPage /> : <HomePage />}</React.StrictMode>,
);
