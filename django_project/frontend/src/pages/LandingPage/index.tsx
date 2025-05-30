import React from 'react';
import {
  Box,
  Button,
  Container,
  Flex,
  Heading,
  Stack,
  Text,
} from '@chakra-ui/react';
import { useGapContext } from '../../contexts/GapContext';


const LandingPage = () => {
    const gapContext = useGapContext();

    const redirectToURL = (url: string) => {
        window.location.href = url;
    };
  return (
    <Box bg="grey.100" minH="100vh" pt={20} pb={10}>
      <Container maxW="7xl">
        <Flex
          direction="column"
          align={{ base: 'center', md: 'flex-start' }}
          textAlign={{ base: 'center', md: 'left' }}
        >
          <Heading
            as="h1"
            fontSize={{ base: '3xl', md: '5xl' }}
            fontWeight="bold"
            mb={4}
            color="black"
          >
            Global Access Platform
          </Heading>
          <Heading
            as="h2"
            fontSize={{ base: 'xl', md: '2xl' }}
            fontWeight="semibold"
            mb={6}
            color="gray.700"
          >
            Agro-Meteorological Intelligence Hub
          </Heading>
          <Text fontSize="md" maxW="2xl" color="gray.600" mb={8}>
            Unlocking Weather Resilience for Smallholder Farming
          </Text>
          <Text fontSize="sm" maxW="2xl" color="gray.500" mb={10}>
            Add description about what is GAP and what can be achieved with it here.
          </Text>

          <Stack direction={{ base: 'column', sm: 'row' }} gap={4}>
            <Button
              bg={'green.400'}
              color="white"
              _hover={{ bg: 'green.500' }}
              px={6}
              rounded="full"
              fontWeight="bold"
            >
              Join Us
            </Button>
            <Button
              variant="outline"
              color="black"
              borderColor="black"
              px={6}
              rounded="full"
              fontWeight="medium"
              _hover={{ bg: 'gray.100' }}
            >
              Explore the Platform
            </Button>
          </Stack>
            <Stack
                direction={{ base: 'column', sm: 'row' }}
                gap={4}
                mt={4}
                justify={{ base: 'center', md: 'flex-start' }}
            >
                <Button
                variant="outline"
                onClick={() => redirectToURL(gapContext.api_swagger_url)}
                >
                API Swagger Docs
                </Button>
                <Button
                variant="outline"
                onClick={() => redirectToURL(gapContext.api_docs_url)}
                >
                API Documentation
                </Button>
            </Stack>
        </Flex>
      </Container>
    </Box>
  );
};

export default LandingPage;
