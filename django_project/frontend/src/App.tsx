import React from 'react';
import { ChakraProvider, Spinner, VStack, Text, Center } from '@chakra-ui/react';
import { GapContextProvider } from '@/context/GapContext';
import { ScrollProvider } from '@/context/ScrollContext';
import ErrorBoundary from '@/components/ErrorBoundary';
import store from '@/app/store';
import system from './theme';
import { Provider } from 'react-redux';
import { RouterProvider } from 'react-router-dom';
import {router} from '@/app/router';
import { Toaster } from '@/components/ui/toaster';
import { useAuthInit } from '@/hooks/useAuthInit';


// Create a separate component that handles auth initialization
const AppContent: React.FC = () => {
  const { hasInitialized } = useAuthInit();

  // Show loading while checking authentication
  if (!hasInitialized) {
    return (
      <Center h="100vh">
        <VStack gap={4}>
          <Spinner
            color="brand.500"
            size="xl"
          />
          <Text fontSize="lg" color="gray.600">
            Loading...
          </Text>
        </VStack>
      </Center>
    );
  }
  return (
    <>
      <RouterProvider router={router} />
      <Toaster />
    </>
  );
};

const App = () => {
  return (
  <React.StrictMode>
    <ChakraProvider value={system}>
      <Provider store={store}>
        <GapContextProvider>
          <ScrollProvider>
            <ErrorBoundary>
              <AppContent />
            </ErrorBoundary>
          </ScrollProvider>
        </GapContextProvider>
      </Provider>
    </ChakraProvider>
  </React.StrictMode>
  )
};

export default App;
