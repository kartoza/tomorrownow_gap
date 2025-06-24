import { useSelector } from 'react-redux';
import { RootState } from '@/app/store';
import { AuthState } from '@/features/auth/type';

export const useAuth = (): AuthState => {
  return useSelector((state: RootState) => state.auth);
};
