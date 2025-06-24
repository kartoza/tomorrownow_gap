import React, { ReactNode } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { Center, Spinner, VStack, Text } from '@chakra-ui/react';
import { useAuth } from '@/hooks/useAuth';
import { LocationState } from '@/types';

interface ProtectedRouteProps {
  children: ReactNode;
}

const ProtectedRoute: React.FC<ProtectedRouteProps> = ({ children }) => {
  const { isAuthenticated, hasInitialized } = useAuth();
  const location = useLocation();

  // Still checking authentication status
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

  // User is not authenticated - redirect to signin
  if (!isAuthenticated) {
    const state: LocationState = { from: location };
    return <Navigate to="/signin" state={state} replace />;
  }

  // User is authenticated - render the protected content
  return <>{children}</>;
};

export default ProtectedRoute;