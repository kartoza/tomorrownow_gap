import React from 'react';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';
import Home from './Home';
import { Toaster } from 'react-hot-toast';

const App = () => (
  <ChakraProvider value={defaultSystem}>
    <GapContextProvider>
      <ErrorBoundary>
        <Home />
        <Toaster
          position="top-center"
          reverseOrder={false}
          toastOptions={{
            duration: 5000,
            style: {
              background: '#1a202c',
              color: '#fff',
            },
          }}
        />
      </ErrorBoundary>
    </GapContextProvider>
  </ChakraProvider>
);

export default App;
