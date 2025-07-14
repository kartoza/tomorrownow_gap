import { useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from './useAuth';
import { LocationState } from '../types';

interface UseProtectedRouteReturn {
  isAuthenticated: boolean;
  hasInitialized: boolean;
}

export const useProtectedRoute = (): UseProtectedRouteReturn => {
  const { isAuthenticated, hasInitialized } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  useEffect(() => {
    if (hasInitialized && !isAuthenticated) {
      navigate('/signin', { 
        state: { from: location } as LocationState,
        replace: true 
      });
    }
  }, [isAuthenticated, hasInitialized, navigate, location]);

  return { isAuthenticated, hasInitialized };
};