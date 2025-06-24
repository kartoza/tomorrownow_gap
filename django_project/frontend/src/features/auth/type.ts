import { User } from '../../types';

export type FormType = "signin" | "signup" | "forgotPassword" | "resetPassword";

export interface AuthState {
  user: User | null;
  token: string | null;
  loading: boolean;
  error: string | null;
  message: string | null;
  isAuthenticated: boolean;
  isAdmin: boolean;
  hasInitialized: boolean;
  pages: string[];
}

