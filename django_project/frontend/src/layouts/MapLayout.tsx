import { Outlet } from 'react-router-dom'
import { Flex, Box } from '@chakra-ui/react'
import Navigation from '@/components/Navigation'
import usePageTracking from '@/hooks/usePageTracking'
import { MapComponent } from '@/features/map/Map'

export const MapLayout = () => {
  usePageTracking();
  return (
    <Flex direction="column" minH="100vh" w="full">
      <Navigation />
      <Box as="main" flexGrow={1} h="full" minHeight={0} display={'flex'}>
        <Box w="350px" bg="white" boxShadow="md" overflowY="auto">
            {/* Left Side */}
            <Outlet />
        </Box>
        <Box
            flexGrow={1}
            display='flex'
            flexDirection='column'
          >
            {/* Map Side */}
            <MapComponent />
          </Box>
      </Box>
    </Flex>
  )
}