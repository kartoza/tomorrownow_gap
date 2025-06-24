import React, { useEffect, useRef } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Text } from '@chakra-ui/react';
import maplibregl, {IControl} from 'maplibre-gl';
import MapboxDraw from '@mapbox/mapbox-gl-draw';
import 'maplibre-gl/dist/maplibre-gl.css';
import '@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css';

import { RootState } from '@/app/store';
import { setDrawingMode, clearBoundingBox, setBoundingBox } from './mapSlice';
import { BoundingBox, customDrawStyles } from './types';


export const MapComponent: React.FC = () => {
  const dispatch = useDispatch();
  const { isDrawingMode, boundingBox } = useSelector(
    (state: RootState) => state.map
  );
  
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const draw = useRef<MapboxDraw | null>(null);

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

    // Initialize MapboxDraw
    draw.current = new MapboxDraw({
      styles: customDrawStyles,
      displayControlsDefault: false,
      controls: {
        polygon: false,
        line_string: false,
        point: false,
        trash: true,
      }
    });

    // Simple rectangle drawing using map click events
    let rectangleStartPoint: [number, number] | null = null;
    let isDrawingRectangle = false;
    let tempStartPointId: string | null = null;

    const handleMapClick = (e: maplibregl.MapMouseEvent) => {
      if (!isDrawingRectangle) return;

      if (!rectangleStartPoint) {
        // First click - set start point
        rectangleStartPoint = [e.lngLat.lng, e.lngLat.lat];

        // Add temporary point to show where user clicked
        const tempPoint = {
          type: 'Feature' as const,
          properties: {
            isTemporary: true
          },
          geometry: {
            type: 'Point' as const,
            coordinates: rectangleStartPoint
          }
        };
        
        const addedFeature = draw.current!.add(tempPoint);
        if (addedFeature && addedFeature.length > 0) {
          tempStartPointId = addedFeature[0];
        }
        
        map.current!.getCanvas().style.cursor = 'crosshair';
      } else {
        // Second click - create rectangle
        const endPoint: [number, number] = [e.lngLat.lng, e.lngLat.lat];
        
        // Create rectangle coordinates
        const coords = [
          [rectangleStartPoint[0], rectangleStartPoint[1]], // SW
          [endPoint[0], rectangleStartPoint[1]],            // SE
          [endPoint[0], endPoint[1]],                       // NE
          [rectangleStartPoint[0], endPoint[1]],            // NW
          [rectangleStartPoint[0], rectangleStartPoint[1]]  // Close the polygon
        ];
        
        // Clear existing features and add rectangle
        draw.current!.deleteAll();
        draw.current!.add({
          type: 'Feature',
          properties: {},
          geometry: {
            type: 'Polygon',
            coordinates: [coords]
          }
        });

        // Calculate and dispatch bounding box
        const bbox: BoundingBox = {
          north: Math.max(rectangleStartPoint[1], endPoint[1]),
          south: Math.min(rectangleStartPoint[1], endPoint[1]),
          east: Math.max(rectangleStartPoint[0], endPoint[0]),
          west: Math.min(rectangleStartPoint[0], endPoint[0]),
        };
        
        dispatch(setBoundingBox(bbox));
        dispatch(setDrawingMode(false));
        
        // Reset drawing state
        rectangleStartPoint = null;
        isDrawingRectangle = false;
        tempStartPointId = null;
        map.current!.getCanvas().style.cursor = '';
      }
    };

    // Store the drawing state handler for cleanup
    (map.current as any)._rectangleHandler = {
      handleMapClick,
      setDrawingState: (drawing: boolean) => {
        isDrawingRectangle = drawing;
        rectangleStartPoint = null;
        tempStartPointId = null;
        if (!drawing) {
          map.current!.getCanvas().style.cursor = '';
        }
      }
    };

    // Add MapboxDraw control container classes 
    const originalOnAdd = draw.current.onAdd.bind(draw.current);
    draw.current.onAdd = (map: any) => {
        const controlContainer = originalOnAdd(map);
        controlContainer.classList.add('maplibregl-ctrl', 'maplibregl-ctrl-group');
        return controlContainer;
    };

    // eslint-disable-next-line no-type-assertion/no-type-assertion
    const typeFudgedDrawControl = draw.current as unknown as IControl;
    map.current.addControl(typeFudgedDrawControl, 'bottom-left');

    // Add map click handler for rectangle drawing
    map.current.on('click', (map.current as any)._rectangleHandler.handleMapClick);

    // Listen for draw delete events
    map.current.on('draw.delete', () => {
      dispatch(clearBoundingBox());
    });

    return () => {
      if (map.current) {
        map.current.remove();
        map.current = null;
      }
    };
  }, []);

  // Handle drawing mode changes from Redux
  useEffect(() => {
    if (map.current && (map.current as any)._rectangleHandler) {
      const handler = (map.current as any)._rectangleHandler;
      
      if (isDrawingMode) {
        // Clear existing drawings first
        draw.current?.deleteAll();
        // Set drawing state
        handler.setDrawingState(true);
        map.current.getCanvas().style.cursor = 'crosshair';
      } else {
        // Exit drawing mode
        handler.setDrawingState(false);
      }
    }
  }, [isDrawingMode]);

  // Handle bounding box clearing from Redux
  useEffect(() => {
    if (draw.current && !boundingBox) {
      draw.current.deleteAll();
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
