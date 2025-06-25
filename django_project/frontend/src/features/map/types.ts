export interface BoundingBox {
  north: number;
  south: number;
  east: number;
  west: number;
}

// test only
export interface FormData {
  location: string;
  category: string;
  description: string;
}

export interface MapState {
  boundingBox: BoundingBox | null;
  isDrawingMode: boolean;
  formData: FormData;
}

// Custom red-themed styles for MapboxDraw
export const customDrawStyles = [
  // Polygon fill - active (being drawn)
  {
    id: 'gl-draw-polygon-fill-active',
    type: 'fill',
    filter: ['all', ['==', 'active', 'true'], ['==', '$type', 'Polygon'], ['!=', 'mode', 'static']],
    paint: {
      'fill-color': '#ff4444',
      'fill-opacity': 0.2
    }
  },
  // Polygon fill - inactive (completed)
  {
    id: 'gl-draw-polygon-fill-inactive',
    type: 'fill',
    filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Polygon'], ['!=', 'mode', 'static']],
    paint: {
      'fill-color': '#dc2626',
      'fill-opacity': 0.15
    }
  },
  // Polygon stroke - active
  {
    id: 'gl-draw-polygon-stroke-active',
    type: 'line',
    filter: ['all', ['==', 'active', 'true'], ['==', '$type', 'Polygon'], ['!=', 'mode', 'static']],
    layout: {
      'line-cap': 'round',
      'line-join': 'round'
    },
    paint: {
      'line-color': '#dc2626',
      'line-width': 3,
      'line-dasharray': [2, 2]
    }
  },
  // Polygon stroke - inactive
  {
    id: 'gl-draw-polygon-stroke-inactive',
    type: 'line',
    filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Polygon'], ['!=', 'mode', 'static']],
    layout: {
      'line-cap': 'round',
      'line-join': 'round'
    },
    paint: {
      'line-color': '#b91c1c',
      'line-width': 2
    }
  },
  // Polygon midpoints (for editing)
  {
    id: 'gl-draw-polygon-midpoint',
    type: 'circle',
    filter: ['all', ['==', '$type', 'Point'], ['==', 'meta', 'midpoint']],
    paint: {
      'circle-radius': 4,
      'circle-color': '#fca5a5',
      'circle-stroke-color': '#dc2626',
      'circle-stroke-width': 2
    }
  },
  // Vertices (corner points)
  {
    id: 'gl-draw-polygon-and-line-vertex-stroke-inactive',
    type: 'circle',
    filter: ['all', ['==', 'meta', 'vertex'], ['==', '$type', 'Point'], ['!=', 'mode', 'static']],
    paint: {
      'circle-radius': 6,
      'circle-color': '#ffffff',
      'circle-stroke-color': '#dc2626',
      'circle-stroke-width': 3
    }
  },
  // Active vertices
  {
    id: 'gl-draw-polygon-and-line-vertex-active',
    type: 'circle',
    filter: ['all', ['==', 'meta', 'vertex'], ['==', '$type', 'Point'], ['!=', 'mode', 'static']],
    paint: {
      'circle-radius': 7,
      'circle-color': '#dc2626',
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': 2
    }
  },
  // Line strings (if needed)
  {
    id: 'gl-draw-line-inactive',
    type: 'line',
    filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'LineString'], ['!=', 'mode', 'static']],
    layout: {
      'line-cap': 'round',
      'line-join': 'round'
    },
    paint: {
      'line-color': '#dc2626',
      'line-width': 3
    }
  },
  // Active line
  {
    id: 'gl-draw-line-active',
    type: 'line',
    filter: ['all', ['==', '$type', 'LineString'], ['==', 'active', 'true'], ['!=', 'mode', 'static']],
    layout: {
      'line-cap': 'round',
      'line-join': 'round'
    },
    paint: {
      'line-color': '#ff4444',
      'line-width': 3,
      'line-dasharray': [2, 2]
    }
  },
  // Points
  {
    id: 'gl-draw-point-point-stroke-inactive',
    type: 'circle',
    filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Point'], ['==', 'meta', 'feature'], ['!=', 'mode', 'static']],
    paint: {
      'circle-radius': 4,
      'circle-opacity': 1,
      'circle-color': '#ffffff',
      'circle-stroke-color': '#dc2626',
      'circle-stroke-width': 3
    }
  },
  // Active points
  {
    id: 'gl-draw-point-active',
    type: 'circle',
    filter: ['all', ['==', '$type', 'Point'], ['==', 'active', 'true'], ['!=', 'mode', 'static']],
    paint: {
      'circle-radius': 6,
      'circle-color': '#dc2626',
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': 2
    }
  },
  // Static features (read-only)
  {
    id: 'gl-draw-polygon-fill-static',
    type: 'fill',
    filter: ['all', ['==', 'mode', 'static'], ['==', '$type', 'Polygon']],
    paint: {
      'fill-color': '#b91c1c',
      'fill-opacity': 0.1
    }
  },
  {
    id: 'gl-draw-polygon-stroke-static',
    type: 'line',
    filter: ['all', ['==', 'mode', 'static'], ['==', '$type', 'Polygon']],
    layout: {
      'line-cap': 'round',
      'line-join': 'round'
    },
    paint: {
      'line-color': '#7f1d1d',
      'line-width': 2
    }
  }
];

