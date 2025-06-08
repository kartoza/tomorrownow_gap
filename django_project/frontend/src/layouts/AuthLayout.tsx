import { Outlet } from 'react-router-dom'
import { Flex, Box } from '@chakra-ui/react'
import Navigation from '@/components/Navigation'
import HeroSection from '@/features/auth/HeroSection'
import { Toaster } from "@/components/ui/toaster"


export const AuthLayout = () => {
  return (
    <Flex direction="column" minH="100vh" w="full">
      <Navigation />
      <Box as="main" flex="1" h="full" display={'flex'}>
        <Flex direction="column" w="full" flex={1}>
            <HeroSection>
                <Outlet />
            </HeroSection>
        </Flex>
      </Box>
      {/* No footer in auth layout. */}
    </Flex>
  )
}