import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import axios from 'axios';
import { setCSRFToken } from '../../utils/csrfUtils'

interface User {
  username: string;
  email: string;
}

interface AuthState {
  user: User | null;
  token: string | null;
  loading: boolean;
  error: string | null;
  message: string | null;
  isAuthenticated: boolean;
  isAdmin: boolean;
}

const initialState: AuthState = {
  user: null,
  token: null,
  loading: false,
  error: null,
  message: null,
  isAuthenticated: false,
  isAdmin: false
};

const authSlice = createSlice({
  name: 'auth',
  initialState,
  reducers: {
    loginStart: (state) => {
      state.loading = true;
      state.error = null;
      state.message = null;
    },
    loginSuccess: (state, action: PayloadAction<{ is_admin: boolean; user: User; token: string }>) => {
      state.loading = false;
      state.user = action.payload.user;
      state.token = action.payload.token;
      state.isAuthenticated = true;
      state.isAdmin = action.payload.is_admin;
      state.message = 'Login successful';
    },
    loginFailure: (state, action: PayloadAction<string>) => {
      state.loading = false;
      state.error = action.payload;
      state.message = null;
      state.isAuthenticated = false;
    },
    logout: (state) => {
      state.user = null;
      state.token = null;
      state.loading = false;
      state.error = null;
      state.isAuthenticated = false;
    },
    setAuthenticationStatus: (state, action: PayloadAction<boolean>) => {
      state.isAuthenticated = action.payload;
    },
    setUser: (state, action: PayloadAction<User>) => {
      state.user = action.payload;
    },
    setMessage: (state, action: PayloadAction<string>) => {
      state.message = action.payload;
      state.error = null;
    },
  },
});

export const { loginStart, loginSuccess, loginFailure, logout, setUser, setMessage } = authSlice.actions;

export const loginUser = (email: string, password: string) => async (dispatch: any) => {
  dispatch(loginStart());
  try {
    setCSRFToken();
    const response = await axios.post('/auth/login/', { email, password});
    const token = response.data.key;
    localStorage.setItem('auth_token', token);
    axios.defaults.headers['Authorization'] = `Token ${token}`;
    dispatch(loginSuccess({ user: response.data.user, token, is_admin: response.data.is_admin }));
  } catch (error: any) {
    dispatch(loginFailure(error.response?.data?.non_field_errors[0] || 'Error logging in'));
  }
};

export const checkLoginStatus = () => async (dispatch: any) => {
  const token = localStorage.getItem('auth_token');
  if (token) {
    axios.defaults.headers['Authorization'] = `Token ${token}`;
    try {
      const response = await axios.get('/api/auth/check-token/');
      dispatch(loginSuccess({ user: response.data.user, token, is_admin: response.data.is_admin }));
      return;
    } catch {
      console.warn("Token validation failed, falling back to user info check.");
    }
  }
  try {
    setCSRFToken();
    const response = await axios.post("/auth/user/", { credentials: "include" });
    if (response.data.is_authenticated) {
      dispatch(loginSuccess({ user: response.data.user, token: null, is_admin: response.data.is_admin }));
    } else {
      await dispatch(logoutUser());
    }
  } catch (error) {
    console.error("User info validation failed:", error);
    await dispatch(logoutUser());
  }
};

export const logoutUser = () => async (dispatch: any) => {
  localStorage.clear();
  try {
    await axios.post('/api/logout/', {}, { withCredentials: true });
    axios.defaults.headers['Authorization'] = '';
    setCSRFToken();
    dispatch(logout());
    window.location.href = '/';
  } catch (e) {
    console.error(e);
  }
};

export const resetPasswordRequest = (email: string) => async (dispatch: any) => {
  try {
    setCSRFToken();
    await axios.post('/password-reset/', { email });
  } catch (error: any) {
    const errorMessage = error.response?.data?.error || 'Error sending password reset email';
    dispatch(loginFailure(errorMessage));
  }
};

export const resetPasswordConfirm = (uid: string, token: string, newPassword: string) => async (dispatch: any) => {
  dispatch(loginStart());
  try {
    setCSRFToken();
    const url = `/password-reset/confirm/${uid}/${token}/`;
    const response = await axios.post(url, { new_password: newPassword });
    if (response.data?.message) {
      dispatch(loginFailure(response.data.message));
    } else {
      dispatch(loginFailure(response.data?.error));
    }
  } catch (error: any) {
    dispatch(loginFailure(error.response?.data?.error || 'Error resetting password'));
  }
};

export const registerUser = (email: string, password: string, repeatPassword: string) => async (dispatch: any) => {
  dispatch(loginStart());
  const errorMessages = [];
  if (password !== repeatPassword) errorMessages.push("Passwords do not match.");
  if (password.length < 6) errorMessages.push("Password must be at least 6 characters.");
  if (errorMessages.length > 0) {
    dispatch(loginFailure(errorMessages.join(' ')));
    return;
  }
  try {
    setCSRFToken();
    const response = await axios.post('/auth/registration/', {
      email,
      password1: password,
      password2: repeatPassword
    });
    if (response.data?.errors) {
      dispatch(loginFailure(response.data.errors.join(' ')));
    } else if (response.data?.detail) {
      dispatch(setMessage(response.data.detail || "Verification email sent."));
    }
  } catch (error: any) {
    if (error.response) {
      const { data, status } = error.response;
  
      if (status === 400) {
        // Handle specific form field errors
        if (data.email && Array.isArray(data.email)) {
          dispatch(loginFailure(data.email.join(' ')));
          return;
        }
  
        if (data.errors) {
          dispatch(loginFailure(data.errors.join(' ')));
          return;
        }
      }
  
      dispatch(loginFailure('An unexpected error occurred during registration.'));
    } else {
      dispatch(loginFailure('An unexpected error occurred during registration.'));
    }
  }
};

// export const selectIsLoggedIn = (state: RootState) => state.auth.isAuthenticated;
// export const isAdmin = (state: RootState) => state.auth.isAdmin;
// export const selectAuthLoading = (state: RootState) => state.auth.loading;
// export const selectUserEmail = (state: RootState) => state.auth.user?.email;
// export const selectUsername = (state: RootState) => state.auth.user?.username;

export const {} = authSlice.actions;
export default authSlice.reducer;
