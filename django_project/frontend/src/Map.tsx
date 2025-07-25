import React from 'react';
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


// Create a separate component that handles auth initialization
const AppContent: React.FC = () => {
  return (
    <>
      <RouterProvider router={router} />
    </>
  );
};

interface MapAppProps {
  visible?: boolean;
}

const MapApp = ({ visible }: MapAppProps) => {
  return (
  <React.StrictMode>
    <ChakraProvider value={system}>
      <Provider store={store}>
        <GapContextProvider>
          <ErrorBoundary>
            {visible && <AppContent />}
            {/* Render the app content only if visible is true */}
            {/* This allows the map to be toggled on and off without unmounting */}
          </ErrorBoundary>
        </GapContextProvider>
      </Provider>
    </ChakraProvider>
  </React.StrictMode>
  )
};

// Make React available globally BEFORE building
(window as any).React = require('react');
(window as any).ReactDOM = require('react-dom/client');

// Expose the MapApp component globally
(window as any).MapApp = MapApp;
