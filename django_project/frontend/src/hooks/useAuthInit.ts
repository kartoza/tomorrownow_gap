import { useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { AuthState } from '@/features/auth/type';
import { AppDispatch, RootState } from '@/app/store';
import { fetchUserInfo } from '@/features/auth/authSlice';

interface UseAuthInitReturn {
  hasInitialized: boolean;
  loading: boolean;
  isAuthenticated: boolean;
  user: AuthState['user'];
}

// Custom hook for initializing Django session authentication
export const useAuthInit = (): UseAuthInitReturn => {
  const dispatch = useDispatch<AppDispatch>();
  const { hasInitialized, loading, isAuthenticated, user } = useSelector(
    (state: RootState) => state.auth
  );
  const authCheckInitiated = useRef(false);

  useEffect(() => {
    if (!hasInitialized && !authCheckInitiated.current) {
        authCheckInitiated.current = true; // Prevent multiple dispatches
        // Check if user is authenticated via Django session
        dispatch(fetchUserInfo());
    }
  }, [dispatch, hasInitialized]);

  return { hasInitialized, loading, isAuthenticated, user };
};