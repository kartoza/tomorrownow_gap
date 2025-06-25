import { createSlice, PayloadAction, createAsyncThunk } from '@reduxjs/toolkit';
import axios from 'axios';
import { setCSRFToken } from '@/utils/csrfUtils'
import { User } from '@/types';
import { AppDispatch } from '@/app/store';
import { AuthState } from './type';

const initialState: AuthState = {
  user: null,
  token: null,
  loading: false,
  error: null,
  message: null,
  isAuthenticated: false,
  isAdmin: false,
  hasInitialized: false, // Track if user info has been fetched
  pages: [],
};

// Async thunk for user logs in
export const loginUser = createAsyncThunk(
  'auth/loginUser',
  async ({ email, password }: { email: string; password: string }, { dispatch, rejectWithValue }) => {
    try {
      setCSRFToken();
      const response = await axios.post('/auth/login/', { email, password });
      return response.data;
    } catch (error: any) {
      const errorMessage = error.response?.data?.['detail'] || 'Error logging in';
      return rejectWithValue(errorMessage);
    }
  }
);

// Async thunk to fetch user info and check authentication status
export const fetchUserInfo = createAsyncThunk(
  'auth/fetchUserInfo',
  async ({}, { dispatch, rejectWithValue }) => {
    try {
      const response = await axios.get(`/api/user-info/`);
      return response.data;
    } catch (error: any) {
      return rejectWithValue(error.response?.data['detail'] || 'Failed to fetch status');
    }
  }
);

export const logoutUser = createAsyncThunk(
  'auth/logoutUser',
  async (_, { dispatch, rejectWithValue }) => {
    localStorage.clear();
    try {
      setCSRFToken();
      await axios.post('/auth/logout/', {}, { withCredentials: true });
      return true; // Indicate successful logout
    } catch (e) {
      console.error(e);
      return rejectWithValue(e.response?.data['detail'] || 'Failed to logout');
    }
  }
)

// Request to reset password
export const resetPasswordRequest = (email: string) => async (dispatch: AppDispatch) => {
  try {
    setCSRFToken();
    const res = await axios.post('/password-reset/', { email });
    dispatch(setMessage(res.data.message));
  } catch (message) {
    const errorMessage = message.response?.data?.message || 'Error sending password reset email';
    dispatch(setMessage(errorMessage));
  }
};

// Request to confirm password reset
export const resetPasswordConfirm = createAsyncThunk<
  string,
  { uid: string; token: string; password: string },
  { rejectValue: string }
>(
  "auth/resetPasswordConfirm",
  async ({ uid, token, password }, { rejectWithValue }) => {
    try {
      setCSRFToken();
      await axios.post(`/password-reset/confirm/${uid}/${token}/`, {
        new_password: password,
      });
      return "Password reset successfully.";
    } catch (err: any) {
      return rejectWithValue(
        err.response?.data?.detail || "Error resetting password."
      );
    }
  }
);

export const fetchPermittedPages = createAsyncThunk<
  string[],
  void,
  { rejectValue: string }
>(
  'auth/fetchPermittedPages',
  async (_, { rejectWithValue }) => {
    try {
      const res = await axios.get<{ pages: string[] }>('/api/permitted-pages/');
      return res.data.pages;
    } catch (err: any) {
      return rejectWithValue('Could not load permissions');
    }
  }
);

const authSlice = createSlice({
  name: 'auth',
  initialState,
  reducers: {
    loginStart: (state) => {
      state.loading = true;
      state.error = null;
      state.message = null;
    },
    loginSuccess: (state, action: PayloadAction<{ is_admin: boolean; user: User; token: string;}>) => {
      state.loading = false;
      state.user = action.payload.user;
      state.token = action.payload.token;
      state.isAuthenticated = true;
      state.isAdmin = action.payload.is_admin;
      state.message = 'Login successful';
      state.hasInitialized = true; // Mark as initialized after successful login
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
    clearFeedback(state) {
      state.message = null;
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchUserInfo.pending, (state) => {
        state.loading = true;
        state.error = null;
        state.hasInitialized = false; // Reset initialization state on fetch
      })
      .addCase(fetchUserInfo.fulfilled, (state, action) => {
        state.loading = false;
        state.user = action.payload;
        state.isAuthenticated = true;
        state.isAdmin = action.payload.is_superuser || false; // Assuming is_superuser indicates admin status
        state.hasInitialized = true; // Mark as initialized after fetching user info
        state.pages = action.payload.pages || []; // Assuming pages are part of user info
      })
      .addCase(fetchUserInfo.rejected, (state, action) => {
        state.loading = false;
        state.error = null;
        state.isAuthenticated = false;
        state.user = null;
        state.hasInitialized = true; // Still mark as initialized even if fetch fails
      })
      .addCase(logoutUser.pending, (state) => {
        state.loading = true;
        state.error = null;
        state.message = null;
      })
      .addCase(logoutUser.fulfilled, (state) => {
        state.loading = false;
        state.user = null;
        state.token = null;
        state.isAuthenticated = false;
        state.isAdmin = false;
        state.message = 'Logout successful';
        state.hasInitialized = true; // Mark as initialized after logout
      }
      )
      .addCase(logoutUser.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload as string;
        state.message = null;
        state.hasInitialized = false; // refetch user info on next login attempt
      }
      )
      .addCase(loginUser.pending, (state) => {
        state.loading = true;
        state.error = null;
        state.message = null;
      })
      .addCase(loginUser.fulfilled, (state, action) => {
        state.loading = false;
        state.user = action.payload.user;
        state.token = null; // Assuming token is not returned in this case
        state.isAuthenticated = true;
        state.isAdmin = action.payload.user.is_superuser || false; // Assuming is_superuser indicates admin status
        state.message = 'Login successful';
        state.hasInitialized = true; // Mark as initialized after successful login
        state.pages = action.payload.pages || [];
      }
      )
      .addCase(loginUser.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload as string;
        state.message = null;
        state.isAuthenticated = false;
        state.user = null;
        state.hasInitialized = true; // Still mark as initialized even if login fails
      }
      )
      .addCase(resetPasswordConfirm.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(
        resetPasswordConfirm.fulfilled,
        (state, action: PayloadAction<string>) => {
          state.loading = false;
          state.error = null;
        }
      )
      .addCase(resetPasswordConfirm.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload || action.error.message || null;
      })
      .addCase(fetchPermittedPages.fulfilled, (state, action) => {
        state.pages = action.payload;
      })
      .addCase(fetchPermittedPages.rejected, (state) => {
        state.pages = [];
      });
  },
});

export const { setUser, setMessage, clearFeedback } = authSlice.actions;

// export const loginUser = (email: string, password: string) => async (dispatch: any) => {
//   dispatch(loginStart());
//   try {
//     setCSRFToken();
//     const response = await axios.post('/auth/login/', { email, password});
//     dispatch(loginSuccess({ user: response.data.user, token: null, is_admin: response.data.user.is_superuser }));
//   } catch (error: any) {
//     dispatch(loginFailure(error.response?.data?.non_field_errors[0] || 'Error logging in'));
//   }
// };

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

export const {loginFailure, loginStart} = authSlice.actions;
export default authSlice.reducer;
