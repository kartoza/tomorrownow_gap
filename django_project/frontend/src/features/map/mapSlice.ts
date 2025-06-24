import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { BoundingBox, FormData, MapState } from './types';

const initialState: MapState = {
  boundingBox: null,
  isDrawingMode: false,
  formData: {
    location: '',
    category: '',
    description: '',
  },
};

const mapSlice = createSlice({
  name: 'map',
  initialState,
  reducers: {
    setBoundingBox: (state, action: PayloadAction<BoundingBox | null>) => {
      state.boundingBox = action.payload;
    },
    setDrawingMode: (state, action: PayloadAction<boolean>) => {
      state.isDrawingMode = action.payload;
    },
    updateFormData: (state, action: PayloadAction<Partial<FormData>>) => {
      state.formData = { ...state.formData, ...action.payload };
    },
    clearBoundingBox: (state) => {
      state.boundingBox = null;
    },
    resetForm: (state) => {
      state.formData = initialState.formData;
      state.boundingBox = null;
      state.isDrawingMode = false;
    },
  },
});

export const {
  setBoundingBox,
  setDrawingMode,
  updateFormData,
  clearBoundingBox,
  resetForm,
} = mapSlice.actions;

export default mapSlice.reducer;

