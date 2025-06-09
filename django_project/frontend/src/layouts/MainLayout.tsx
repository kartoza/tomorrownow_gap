import { Outlet } from 'react-router-dom'
import { Flex, Box } from '@chakra-ui/react'
import Navigation from '@/components/Navigation'
import MainFooter from '@/components/MainFooter'
import usePageTracking from '@/hooks/usePageTracking'

export const MainLayout = () => {
  usePageTracking();
  return (
    <Flex direction="column" minH="100vh" w="full">
      <Navigation />
      <Box as="main" flex="1">
        <Outlet />
      </Box>
      <MainFooter />
    </Flex>
  )
}