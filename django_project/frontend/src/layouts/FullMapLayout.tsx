import { useEffect } from 'react'
import { useDispatch } from 'react-redux';
import { Flex, Box } from '@chakra-ui/react'
import { MapComponent } from '@/features/map/Map'
import { setDrawingMode, setBoundingBox } from '@/features/map/mapSlice';
import { BoundingBox } from '@/features/map/types';

export const FullMapLayout = () => {
  const dispatch = useDispatch();
  useEffect(() => {
    // set drawing mode when the layout mounts
    dispatch(setDrawingMode(true));
  }, [dispatch]);

  const handleBoundingBoxChange = (bbox: BoundingBox) => {
    if (!bbox) {
      setTimeout(() => {
        dispatch(setDrawingMode(true));
      }, 300);
    }

    let t = document.querySelector("#operations-Weather_\\\\\\&_Climate_Data-get-measurement > div.no-margin > div > div.opblock-section > div.parameters-container > div > table > tbody > tr:nth-child(15) > td.parameters-col_description > input[type=text]") as HTMLInputElement;
    if (t) {
      t.value = bbox
        ? `${bbox.west.toFixed(2)}, ${bbox.south.toFixed(2)}, ${bbox.east.toFixed(2)}, ${bbox.north.toFixed(2)}`
        : '';
    }
  };

  return (
    <Flex direction="column" minH="100%" w="full">
      <Box as="main" flexGrow={1} h="full" minHeight={0} display={'flex'}>
        <Box
            flexGrow={1}
            display='flex'
            flexDirection='column'
          >
            {/* Map Side */}
            <MapComponent notifyBoundingBoxChange={handleBoundingBoxChange} />
          </Box>
      </Box>
    </Flex>
  )
}