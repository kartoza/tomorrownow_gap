import maplibregl from 'maplibre-gl';
import MapboxDraw from '@mapbox/mapbox-gl-draw';

import { BoundingBox, customDrawStyles } from './types';

interface BboxDrawControlOptions {
  onBoundingBoxChange?: (bbox: BoundingBox | null) => void;
  onDrawingModeChange?: (isDrawing: boolean) => void;
}

export class BboxDrawControl implements maplibregl.IControl {
  private map: maplibregl.Map | null = null;
  private draw: MapboxDraw | null = null;
  private container: HTMLDivElement | null = null;
  private isDrawingRectangle: boolean = false;
  private rectangleStartPoint: [number, number] | null = null;
  private tempStartPointId: string | null = null;
  private options: BboxDrawControlOptions;

  constructor(options: BboxDrawControlOptions = {}) {
    this.options = options;
    this.handleMapClick = this.handleMapClick.bind(this);
  }

  onAdd(map: maplibregl.Map): HTMLElement {
    this.map = map;
    this.container = document.createElement('div');
    this.container.className = 'maplibregl-ctrl maplibregl-ctrl-group';

    // Initialize MapboxDraw with custom red styles
    this.draw = new MapboxDraw({
      displayControlsDefault: false,
      controls: {
        polygon: false,
        line_string: false,
        point: false,
        trash: true,
      },
      styles: this.getCustomStyles(),
    });

    // Add MapboxDraw to the map
    const typeFudgedDrawControl = this.draw as unknown as maplibregl.IControl;
    const originalOnAdd = this.draw.onAdd.bind(this.draw);
    this.draw.onAdd = (map: any) => {
        const controlContainer = originalOnAdd(map);
        controlContainer.classList.add('maplibregl-ctrl', 'maplibregl-ctrl-group');
        return controlContainer;
    };
    map.addControl(typeFudgedDrawControl, 'bottom-left');

    // Add map click handler
    map.on('click', this.handleMapClick);

    // Listen for draw delete events
    map.on('draw.delete', () => {
      this.options.onBoundingBoxChange?.(null);
    });

    return this.container;
  }

  onRemove(): void {
    if (this.map) {
      this.map.off('click', this.handleMapClick);
      if (this.draw) {
        const typeFudgedDrawControl = this.draw as unknown as maplibregl.IControl;
        this.map.removeControl(typeFudgedDrawControl);
      }
    }
    this.container?.remove();
    this.map = null;
    this.draw = null;
    this.container = null;
  }

  private getCustomStyles() {
    return customDrawStyles;
  }

  private handleMapClick(e: maplibregl.MapMouseEvent): void {
    if (!this.isDrawingRectangle || !this.draw) return;

    if (!this.rectangleStartPoint) {
      // First click - set start point and show visual marker
      this.rectangleStartPoint = [e.lngLat.lng, e.lngLat.lat];
      
      // Add temporary point to show where user clicked
      const tempPoint = {
        type: 'Feature' as const,
        properties: {
          isTemporary: true
        },
        geometry: {
          type: 'Point' as const,
          coordinates: this.rectangleStartPoint
        }
      };
      
      const addedFeature = this.draw.add(tempPoint);
      if (addedFeature && addedFeature.length > 0) {
        this.tempStartPointId = addedFeature[0];
      }
      
      if (this.map) {
        this.map.getCanvas().style.cursor = 'crosshair';
      }
    } else {
      // Second click - create rectangle
      const endPoint: [number, number] = [e.lngLat.lng, e.lngLat.lat];
      
      // Create rectangle coordinates
      const coords = [
        [this.rectangleStartPoint[0], this.rectangleStartPoint[1]], // SW
        [endPoint[0], this.rectangleStartPoint[1]],                 // SE
        [endPoint[0], endPoint[1]],                                 // NE
        [this.rectangleStartPoint[0], endPoint[1]],                 // NW
        [this.rectangleStartPoint[0], this.rectangleStartPoint[1]]  // Close
      ];
      
      // Clear all features and add rectangle
      this.draw.deleteAll();
      this.draw.add({
        type: 'Feature',
        properties: {},
        geometry: {
          type: 'Polygon',
          coordinates: [coords]
        }
      });

      // Calculate bounding box
      const bbox: BoundingBox = {
        north: Math.max(this.rectangleStartPoint[1], endPoint[1]),
        south: Math.min(this.rectangleStartPoint[1], endPoint[1]),
        east: Math.max(this.rectangleStartPoint[0], endPoint[0]),
        west: Math.min(this.rectangleStartPoint[0], endPoint[0]),
      };
      
      // Notify parent component
      this.options.onBoundingBoxChange?.(bbox);
      this.exitDrawingMode();
    }
  }

  // Public methods
  startDrawingMode(): void {
    if (!this.isDrawingRectangle) {
      this.isDrawingRectangle = true;
      this.rectangleStartPoint = null;
      this.tempStartPointId = null;
      
      // Clear existing drawings
      this.draw?.deleteAll();
      
      if (this.map) {
        this.map.getCanvas().style.cursor = 'crosshair';
      }
      
      this.options.onDrawingModeChange?.(true);
    }
  }

  exitDrawingMode(): void {
    if (this.isDrawingRectangle) {
      this.isDrawingRectangle = false;
      this.rectangleStartPoint = null;
      this.tempStartPointId = null;
      
      if (this.map) {
        this.map.getCanvas().style.cursor = '';
      }
      
      this.options.onDrawingModeChange?.(false);
    }
  }

  clearBoundingBox(): void {
    this.draw?.deleteAll();
    this.options.onBoundingBoxChange?.(null);
  }

  isDrawing(): boolean {
    return this.isDrawingRectangle;
  }
}

