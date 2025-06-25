import React, { useEffect, useRef } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Text } from '@chakra-ui/react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import '@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css';

import { RootState } from '@/app/store';
import { setDrawingMode, setBoundingBox } from './mapSlice';
import { BboxDrawControl } from './BboxDrawControl';


export const MapComponent: React.FC = () => {
  const dispatch = useDispatch();
  const { isDrawingMode, boundingBox } = useSelector(
    (state: RootState) => state.map
  );
  
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const bboxControl = useRef<BboxDrawControl | null>(null);

  useEffect(() => {
    if (!mapContainer.current || map.current) return;

    // Initialize map
    map.current = new maplibregl.Map({
      container: mapContainer.current,
      center: [36.817223, -1.286389], // Nairobi coordinates
      zoom: 8,
    });

    // Add OpenStreetMap basemap
    map.current.addSource('BASEMAP', {
        type: "raster",
        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
        tileSize: 256
      }
    )
    map.current.addLayer(
      {
        id: 'BASEMAP',
        source: 'BASEMAP',
        type: "raster"
      },
      map.current.getStyle().layers[0]?.id
    )

    map.current.addControl(new maplibregl.NavigationControl(), 'bottom-left');

    // Initialize BboxDrawControl with callbacks
    bboxControl.current = new BboxDrawControl({
      onBoundingBoxChange: (bbox) => {
        dispatch(setBoundingBox(bbox));
      },
      onDrawingModeChange: (drawing) => {
        dispatch(setDrawingMode(drawing));
      }
    });
    // Add the control to the map
    map.current.addControl(bboxControl.current, 'bottom-left');

    return () => {
      if (map.current) {
        if (bboxControl.current) {
          map.current.removeControl(bboxControl.current);
          bboxControl.current = null;
        }
        map.current.remove();
        map.current = null;
      }
    };
  }, []);

   // Handle drawing mode changes from Redux
  useEffect(() => {
    if (bboxControl.current) {
      if (isDrawingMode && !bboxControl.current.isDrawing()) {
        bboxControl.current.startDrawingMode();
      } else if (!isDrawingMode && bboxControl.current.isDrawing()) {
        bboxControl.current.exitDrawingMode();
      }
    }
  }, [isDrawingMode]);

  // Handle bounding box clearing from Redux
  useEffect(() => {
    if (bboxControl.current && !boundingBox) {
      bboxControl.current.clearBoundingBox();
    }
  }, [boundingBox]);

  return (
    <Box flex="1" position="relative">
      <div
        ref={mapContainer}
        style={{
          width: '100%',
          height: '100%',
        }}
      />
      
      {/* Map Status Indicator */}
      {isDrawingMode && (
        <Box
          position="absolute"
          top={4}
          left="50%"
          transform="translateX(-50%)"
          bg="blue.500"
          color="white"
          px={4}
          py={2}
          rounded="md"
          shadow="md"
          zIndex={1000}
        >
          <Text fontSize="sm" fontWeight="medium">
            🎯 Step 1: Click first corner → Step 2: Click opposite corner
          </Text>
        </Box>
      )}
      
      {/* Map Instructions */}
      <Box
        position="absolute"
        bottom={4}
        right={4}
        bg="white"
        p={3}
        rounded="md"
        shadow="md"
        maxW="250px"
      >
        <Text fontSize="sm" fontWeight="medium" mb={2}>
          Map Controls:
        </Text>
        <Text fontSize="xs" color="gray.600">
          • Use the form to start rectangle drawing<br/>
          • Step 1: Click first corner (red point appears)<br/>
          • Step 2: Click opposite corner (rectangle created)<br/>
          • Use trash button (🗑️) to delete selections<br/>
          • Pan and zoom normally when not drawing
        </Text>
      </Box>
    </Box>
  );
};
