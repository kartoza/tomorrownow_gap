// src/SignupRequest.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { SignupRequestForm } from './pages/SignupRequest';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';

// Mock user data – replace this with actual user context/auth logic if needed
const user = {
  email: '',
  first_name: '',
  last_name: ''
};

const root = createRoot(document.getElementById('app')!);

root.render(
  <ChakraProvider value={defaultSystem}>
    <GapContextProvider>
      <ErrorBoundary>
        <SignupRequestForm user={user} />
      </ErrorBoundary>
    </GapContextProvider>
  </ChakraProvider>
);
