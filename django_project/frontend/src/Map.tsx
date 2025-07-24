import React from 'react';
import { createRoot } from 'react-dom/client';
import { ChakraProvider } from '@chakra-ui/react';
import { GapContextProvider } from '@/context/GapContext';
import ErrorBoundary from '@/components/ErrorBoundary';
import store from '@/app/store';
import system from './theme';
import './styles/index.scss';
import "./styles/font.css";
import { Provider } from 'react-redux';
import { RouterProvider } from 'react-router-dom';
import {router} from '@/app/map_router';
import { Toaster } from '@/components/ui/toaster';


// Create a separate component that handles auth initialization
const AppContent: React.FC = () => {
  return (
    <>
      <RouterProvider router={router} />
      <Toaster />
    </>
  );
};

const MapApp = () => {
  return (
  <React.StrictMode>
    <ChakraProvider value={system}>
      <Provider store={store}>
        <GapContextProvider>
          <ErrorBoundary>
            <AppContent />
          </ErrorBoundary>
        </GapContextProvider>
      </Provider>
    </ChakraProvider>
  </React.StrictMode>
  )
};

const root = createRoot(document.getElementById('map-app')!);
root.render(<MapApp />);
