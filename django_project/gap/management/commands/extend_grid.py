import os
import json
import numpy as np
import duckdb
from django.core.management.base import BaseCommand

from gap.models import Country
from gap.utils.netcdf import find_start_latlng


class Command(BaseCommand):

    lat_metadata = {
        'min': -27,
        'max': 16,
        'inc': 0.03586314,
        'original_min': -4.65013565
    }
    lon_metadata = {
        'min': 21.8,
        'max': 52,
        'inc': 0.036353,
        'original_min': 33.91823667
    }

    def advanced_spatial_filter(
        self,
        conn,
        source_table='point_table',
        target_table='malawi_points_detailed',
        country_name='Malawi'
    ):
        """
        Advanced filtering with additional spatial information
        """
        try: 
            # Drop existing table
            conn.execute(f"DROP TABLE IF EXISTS {target_table};")
            
            # Create table with additional spatial metadata
            advanced_sql = f"""
                CREATE TABLE {target_table} AS
                SELECT 
                    p.*,
                    c.name as country_name,
                    ST_Contains(c.geom, p.geom) as is_interior,
                    ST_Touches(c.geom, p.geom) as is_on_boundary,
                    ST_Distance(p.geom, ST_Boundary(c.geom)) as distance_to_border,
                    ST_X(p.geom) as longitude,
                    ST_Y(p.geom) as latitude
                FROM {source_table} p
                CROSS JOIN country c
                WHERE c.name LIKE '%{country_name}%'
                AND ST_Covers(c.geom, p.geom);
            """
            
            conn.execute(advanced_sql)
            
            # Get results
            count = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            
            # Show boundary vs interior breakdown
            boundary_stats = conn.execute(f"""
                SELECT 
                    is_interior,
                    is_on_boundary,
                    COUNT(*) as point_count
                FROM {target_table}
                GROUP BY is_interior, is_on_boundary
                ORDER BY is_interior DESC, is_on_boundary DESC
            """).fetchall()
            
            print(f"✅ Created advanced table '{target_table}' with {count:,} points")
            print(f"\n=== Interior vs Boundary Breakdown ===")
            for row in boundary_stats:
                location = "Interior" if row[0] else ("Boundary" if row[1] else "Other")
                print(f"{location}: {row[2]:,} points")

            # Show statistics
            self.show_statistics(conn, source_table, target_table, country_name)

            return count
            
        except Exception as e:
            print(f"Error in advanced filtering: {e}")
            raise

    def show_statistics(self, conn, source_table, target_table, country_name):
        """Show statistics about the filtering operation"""
        
        try:
            # Get total points in source table
            total_points = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            
            # Get filtered points count
            filtered_points = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            
            # Calculate percentage
            percentage = (filtered_points / total_points * 100) if total_points > 0 else 0
            
            print(f"\n=== Filtering Statistics ===")
            print(f"Total points in {source_table}: {total_points:,}")
            print(f"Points inside {country_name}: {filtered_points:,}")
            print(f"Percentage inside: {percentage:.2f}%")
            print(f"Points outside: {total_points - filtered_points:,}")
            
            # Show sample of filtered points
            sample_result = conn.execute(f"""
                SELECT * FROM {target_table} 
                LIMIT 5
            """).fetchall()
            
            if sample_result:
                print(f"\n=== Sample of filtered points ===")
                columns = [desc[0] for desc in conn.description]
                print(f"Columns: {', '.join(columns)}")
                for i, row in enumerate(sample_result, 1):
                    print(f"Row {i}: {row}")
            
        except Exception as e:
            print(f"Error showing statistics: {e}")

    def export_simple_geojson(
        self,
        conn,
        table_name='malawi_points_advanced',
        output_file='malawi_points.geojson'
    ):
        """
        Alternative: Export as GeoJSON (more universally supported)
        """
        try: 
            # Create FeatureCollection using SQL aggregation
            featurecollection_sql = f"""
                SELECT json_object(
                    'type', 'FeatureCollection',
                    'features', json_group_array(
                        json_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(geom)::JSON,
                            'properties', json_object(
                                'country_name', country_name,
                                'is_interior', is_interior,
                                'is_on_boundary', is_on_boundary,
                                'distance_to_border', distance_to_border,
                                'longitude', longitude,
                                'latitude', latitude
                            )
                        )
                    )
                ) as feature_collection
                FROM {table_name}
                WHERE geom IS NOT NULL
            """
            
            print(f"Creating GeoJSON FeatureCollection using native SQL to: {output_file}")
        
            # Execute and get the FeatureCollection
            result = conn.execute(featurecollection_sql).fetchone()
            
            if not result or not result[0]:
                print("❌ No data found to export")
                return None
            
            # Write to file
            feature_collection = json.loads(result[0])
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(feature_collection, f, indent=2, ensure_ascii=False)
            
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file) / (1024 * 1024)
                print(f"✅ GeoJSON export complete: {file_size:.2f} MB")
                return output_file
        except Exception as e:
            print(f"❌ GeoJSON export failed: {e}")

    def export_country_to_geojson(self, country, output_file='country.geojson'):
        """
        Export country geometry to GeoJSON format
        """
        try:
            if not country.geometry:
                print(f"❌ No geometry found for country: {country.name}")
                return None
            
            # Convert geometry to GeoJSON
            geojson = {
                "type": "Feature",
                "geometry": json.loads(country.geometry.geojson),
                "properties": {
                    "name": country.name
                }
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, indent=2, ensure_ascii=False)
            
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file) / (1024 * 1024)
                print(f"✅ Country GeoJSON export complete: {file_size:.2f} MB")
                return output_file
            else:
                print("❌ Export file was not created")
                return None
            
        except Exception as e:
            print(f"❌ Error exporting country to GeoJSON: {e}")

    def create_grid_cells(
        self,
        conn,
        source_table='malawi_points_advanced',
        target_table='malawi_grid_4km',
        cell_size_km=4,
        method='utm_accurate',
        id_prefix='MLW'
    ):
        """
        Create grid cells from point centroids
        
        Args:
            db_path (str): Path to DuckDB database
            source_table (str): Source table with points
            target_table (str): Target table for grid cells
            cell_size_km (float): Size of grid cells in kilometers
            method (str): 'utm_accurate', 'simple_degrees', 'buffer', or 'manual'
        
        Returns:
            int: Number of grid cells created
        """
        try:
            # Check source table
            count_check = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            print(f"Source table '{source_table}' has {count_check:,} points")
            
            if count_check == 0:
                print("❌ No points found in source table")
                return 0
            
            # Drop existing target table
            conn.execute(f"DROP TABLE IF EXISTS {target_table};")
            
            # Calculate cell dimensions
            half_size_km = cell_size_km / 2
            half_size_m = half_size_km * 1000
            half_size_deg = cell_size_km / 111.32  # Approximate degrees (less accurate)
            
            print(f"Creating {cell_size_km}km x {cell_size_km}km grid cells using {method} method...")
            
            if method == 'utm_accurate':
                # Most accurate method using UTM projection
                sql = f"""
                    CREATE TABLE {target_table} AS
                    WITH utm_transformed AS (
                        SELECT 
                            *,
                            ST_Transform(geom, 'EPSG:4326', 'EPSG:32736') as utm_point
                        FROM {source_table}
                        WHERE geom IS NOT NULL
                    ),
                    grid_created AS (
                        SELECT 
                            *,
                            ST_MakeEnvelope(
                                ST_X(utm_point) - {half_size_m},
                                ST_Y(utm_point) - {half_size_m},
                                ST_X(utm_point) + {half_size_m},
                                ST_Y(utm_point) + {half_size_m}
                            ) as utm_grid
                        FROM utm_transformed
                    )
                    SELECT 
                        *,
                        ST_Transform(utm_grid, 'EPSG:32736', 'EPSG:4326') as grid_cell,
                        ST_Area(utm_grid) / 1000000 as grid_area_km2,
                        {cell_size_km * 1000} as cell_size_meters,
                        'utm_accurate' as creation_method,
                        CONCAT('grid_', 
                            CAST(ROUND(ST_X(utm_point) / {cell_size_km * 1000}) AS INTEGER), '_', 
                            CAST(ROUND(ST_Y(utm_point) / {cell_size_km * 1000}) AS INTEGER)
                        ) as grid_id
                    FROM grid_created;
                """
                
            elif method == 'simple_degrees':
                # Simple degree-based approximation
                lat_diff = self.lat_metadata['inc'] / 2
                lon_diff = self.lon_metadata['inc'] / 2
                sql = f"""
                    CREATE TABLE {target_table} AS
                    WITH grid_cells AS (
                        SELECT 
                        *,
                        ST_MakeEnvelope(
                            longitude - {lon_diff:.6f},
                            latitude - {lat_diff:.6f},
                            longitude + {lon_diff:.6f},
                            latitude + {lat_diff:.6f}
                        ) as grid_cell,
                        row_number() OVER () AS cell_id
                        FROM {source_table}
                        WHERE geom IS NOT NULL
                    )
                    SELECT 
                        *,
                        ST_AREA(ST_TRANSFORM(grid_cell, 'EPSG:4326', 'EPSG:3857')) / 1000000 as grid_area_km2,
                        {cell_size_km * 1000} as cell_size_meters,
                        'simple_degrees' as creation_method,
                        CONCAT('{id_prefix}_', cell_id) as grid_id,
                        longitude as origin_longitude,
                        latitude as origin_latitude,
                        ST_X(ST_CENTROID(grid_cell)) as longitude,
                        ST_Y(ST_CENTROID(grid_cell)) as latitude
                    FROM grid_cells;
                """
                
            elif method == 'buffer':
                # Buffer method (creates circular/octagonal cells)
                buffer_deg = half_size_deg * 0.7  # Approximate to get similar area
                sql = f"""
                CREATE TABLE {target_table} AS
                SELECT 
                    *,
                    ST_Buffer(geom, {buffer_deg:.6f}) as grid_cell,
                    {cell_size_km * cell_size_km:.2f} as grid_area_km2,
                    {cell_size_km * 1000} as cell_size_meters,
                    'buffer_method' as creation_method,
                    CONCAT('grid_buf_', 
                        CAST(ROUND(longitude, 3) * 1000 AS INTEGER), '_', 
                        CAST(ROUND(latitude, 3) * 1000 AS INTEGER)
                    ) as grid_id
                FROM {source_table}
                WHERE geom IS NOT NULL;
                """
                
            else:  # manual polygon
                sql = f"""
                CREATE TABLE {target_table} AS
                SELECT 
                    *,
                    ST_GeomFromText(
                        'POLYGON((' || 
                        (longitude - {half_size_deg:.6f}) || ' ' || (latitude - {half_size_deg:.6f}) || ',' ||
                        (longitude + {half_size_deg:.6f}) || ' ' || (latitude - {half_size_deg:.6f}) || ',' ||
                        (longitude + {half_size_deg:.6f}) || ' ' || (latitude + {half_size_deg:.6f}) || ',' ||
                        (longitude - {half_size_deg:.6f}) || ' ' || (latitude + {half_size_deg:.6f}) || ',' ||
                        (longitude - {half_size_deg:.6f}) || ' ' || (latitude - {half_size_deg:.6f}) ||
                        '))', 4326
                    ) as grid_cell,
                    {cell_size_km * cell_size_km:.2f} as grid_area_km2,
                    {cell_size_km * 1000} as cell_size_meters,
                    'manual_polygon' as creation_method,
                    CONCAT('grid_man_', 
                        CAST(ROUND(longitude, 3) * 1000 AS INTEGER), '_', 
                        CAST(ROUND(latitude, 3) * 1000 AS INTEGER)
                    ) as grid_id
                FROM {source_table}
                WHERE geom IS NOT NULL;
                """
            
            # Execute the creation query
            conn.execute(sql)
            
            # Get count of created cells
            created_count = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            
            # Show statistics
            stats = conn.execute(f"""
                SELECT 
                    creation_method,
                    COUNT(*) as grid_count,
                    AVG(grid_area_km2) as avg_area_km2,
                    MIN(grid_area_km2) as min_area_km2,
                    MAX(grid_area_km2) as max_area_km2
                FROM {target_table}
                GROUP BY creation_method
            """).fetchall()
            
            print(f"✅ Created {created_count:,} grid cells")
            print(f"📊 Statistics:")
            for stat in stats:
                method_name, count, avg_area, min_area, max_area = stat
                print(f"   Method: {method_name}")
                print(f"   Count: {count:,} cells") 
                print(f"   Area: {avg_area:.2f} km² (range: {min_area:.2f} - {max_area:.2f})")
            
            return created_count
            
        except Exception as e:
            print(f"❌ Error creating grid cells: {e}")
            raise

    def check_grid_cells(
        self,
        conn,
        table_name='malawi_grid_4km',
        tolerance=0.01  # Tolerance for checking centering
    ):
        sql = f"""
        SELECT ABS(longitude - origin_longitude) AS lon_diff,
               ABS(latitude - origin_latitude) AS lat_diff,
               COUNT(*) AS cell_count
        FROM {table_name}
        GROUP BY lon_diff, lat_diff
        HAVING lon_diff > {tolerance} OR lat_diff > {tolerance}
        ORDER BY lon_diff, lat_diff;
        """
        conn.execute(sql)
        results = conn.execute(sql).fetchall()
        if not results:
            print(f"✅ All grid cells in '{table_name}' are correctly centered.")
            return True
        else:
            print(f"❌ Found discrepancies in grid cells in '{table_name}':")
            for row in results:
                lon_diff, lat_diff, count = row
                print(f"   • Longitude diff: {lon_diff:.6f}, Latitude diff: {lat_diff:.6f}, Count: {count}")
            return False

    def export_grid_to_files(
        self,
        conn,
        table_name='malawi_grid_4km',
        output_base='malawi_4km_grid'
    ):
        """
        Export grid cells to various formats for visualization
        """
        
        print(f"=== Exporting grid cells to files ===")
        
        try:            
            # Get grid data
            grids = conn.execute(f"""
                SELECT 
                    json_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(grid_cell)::JSON,
                        'properties', json_object(
                            'grid_id', grid_id,
                            'point_count', 1,
                            'grid_area_km2', grid_area_km2,
                            'country_name', country_name,
                            'is_interior', is_interior,
                            'creation_method', creation_method,
                            'longitude', longitude,
                            'latitude', latitude,
                            'origin_longitude', origin_longitude,
                            'origin_latitude', origin_latitude
                        )
                    ) as feature
                FROM {table_name}
                WHERE grid_cell IS NOT NULL
            """).fetchall()
            
            # Create FeatureCollection           
            features = [json.loads(row[0]) for row in grids]
            feature_collection = {
                "type": "FeatureCollection",
                "features": features
            }
            
            # Write GeoJSON
            geojson_file = f"{output_base}.geojson"
            with open(geojson_file, 'w') as f:
                json.dump(feature_collection, f, indent=2)
            
            # CSV export
            csv_file = f"{output_base}.csv"
            conn.execute(f"""
                COPY (
                    SELECT 
                        grid_id as locationid,
                        0 as elevation,
                        grid_id as name,
                        ST_AsText(grid_cell) as shapewkt
                    FROM {table_name}
                ) TO '{csv_file}' WITH (FORMAT 'csv', HEADER true);
            """)
            
            conn.close()
            
            print(f"✅ Exported grid cells:")
            if os.path.exists(geojson_file):
                size = os.path.getsize(geojson_file) / (1024 * 1024)
                print(f"   • GeoJSON: {geojson_file} ({size:.1f} MB)")
            if os.path.exists(csv_file):
                size = os.path.getsize(csv_file) / (1024 * 1024) 
                print(f"   • CSV: {csv_file} ({size:.1f} MB)")
            
            return [geojson_file, csv_file]
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
            return []

    def handle(self, *args, **options): 
        country = Country.objects.get(name='Zambia')
        country_name = country.name.lower()
        # get bounding box of country
        print(f'Country: {country.name}')
        # xmin, ymin, xmax, ymax
        country_bbox = country.geometry.extent
        print(f'Bounding Box: {country_bbox}')
        self.export_country_to_geojson(country, output_file=f'{country_name}.geojson')

        # expand lat and lon
        min_lat = find_start_latlng(self.lat_metadata)
        min_lon = find_start_latlng(self.lon_metadata)
        new_lat = np.arange(
            min_lat, self.lat_metadata['max'] + self.lat_metadata['inc'],
            self.lat_metadata['inc']
        )
        new_lon = np.arange(
            min_lon, self.lon_metadata['max'] + self.lon_metadata['inc'],
            self.lon_metadata['inc']
        )

        # filter new_lat and new_lon to be within country bbox
        new_lat = new_lat[(new_lat >= country_bbox[1]) & (new_lat <= country_bbox[3])]
        new_lon = new_lon[(new_lon >= country_bbox[0]) & (new_lon <= country_bbox[2])]
        print(f'New Lat: {new_lat}')
        print(f'New Lon: {new_lon}')

        # init duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        # create country table
        con.execute(
            """
            CREATE TABLE country (
                name TEXT,
                geom GEOMETRY
            );
            """
        )

        # write country to csv file
        wkt_escaped = (country.geometry.wkt or '').replace('"', '""')
        with open('country_wkt.csv', 'w') as f:
            f.write("name,wkt\n")
            f.write(f'{country.name},"{wkt_escaped}"\n')


        # Insert into DuckDB using ST_GeomFromText
        query = """
        INSERT INTO country (name, geom)
        FROM read_csv_auto('country_wkt.csv', header=true, quote='"', escape='"');
        """
        con.execute(query)

        query = """
            CREATE TABLE point_table AS
            WITH
            latitudes AS (
                SELECT unnest(?) AS lat
            ),
            longitudes AS (
                SELECT unnest(?) AS lon
            )
            SELECT ST_Point(lon, lat) AS geom
            FROM latitudes
            CROSS JOIN longitudes;
        """

        con.execute(query, [new_lat, new_lon])

        # Perform advanced spatial filtering
        count = self.advanced_spatial_filter(
            con,
            source_table='point_table',
            target_table=f'{country_name}_points_detailed',
            country_name=country.name
        )
        print(f"Total points after filtering: {count:,}")

        # Export to GeoJSON
        self.export_simple_geojson(
            con,
            table_name=f'{country_name}_points_detailed',
            output_file=f'{country_name}_points.geojson'
        )

        # Create grid cells
        grid_count = self.create_grid_cells(
            con,
            source_table=f'{country_name}_points_detailed',
            target_table=f'{country_name}_grid_4km',
            cell_size_km=4,
            method='simple_degrees',
            id_prefix='ZAM'
        )
        print(f"Total grid cells created: {grid_count:,}")
        # Check grid cells for discrepancies
        if not self.check_grid_cells(
            con,
            table_name=f'{country_name}_grid_4km',
            tolerance=0.001
        ):
            print("❌ Grid cells have discrepancies, please check the data.")
        # Export grid cells to files
        exported_files = self.export_grid_to_files(
            con,
            table_name=f'{country_name}_grid_4km',
            output_base=f'{country_name}_4km_grid'
        )
        print(f"Exported files: {exported_files}")

        # Close the connection
        con.close()
