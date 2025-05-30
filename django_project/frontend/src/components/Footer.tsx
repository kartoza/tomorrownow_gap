import { Box, Flex, Text, Link, Stack } from '@chakra-ui/react';

const Footer = () => {
  return (
    <Box bg="#001912" color="white" px={{ base: 4, md: 10 }} py={10}>
      <Flex
        direction={{ base: 'column', md: 'row' }}
        justify="space-between"
        gap={10}
      >
        {/* Left: About */}
        <Box maxW="sm">
          <Text fontWeight="bold" mb={2}>
            Global Access Platform (GAP)
          </Text>
          <Text fontSize="sm">
            An open-source platform for weather and climate data, empowering
            smallholder farmers worldwide.
          </Text>
        </Box>

        {/* Middle: Resources */}
        <Stack gap={1}>
          <Text fontWeight="bold" mb={2}>
            Resources
          </Text>
          <Link href="#" fontSize="sm" color="gray.200" _hover={{ color: "white" }}>
            Documentation
          </Link>
          <Link href="#" fontSize="sm" color="gray.200" _hover={{ color: "white" }}>
            GitHub Repository
          </Link>
          <Link href="#" fontSize="sm" color="gray.200" _hover={{ color: "white" }}>
            API Reference
          </Link>
          <Link href="#" fontSize="sm" color="gray.200" _hover={{ color: "white" }}>
            Community Forum
          </Link>
        </Stack>

        {/* Right: Contact */}
        <Stack gap={1}>
          <Text fontWeight="bold" mb={2}>
            Contact Us
          </Text>
          <Text fontSize="sm">
            Have questions or want to get involved? Reach out to us.
          </Text>
          <Link href="mailto:contact@gapexplorer.org" fontSize="sm" color="gray.200" _hover={{ color: "white" }}>
            contact@gapexplorer.org
          </Link>
        </Stack>
      </Flex>

      <Box h="1px" bg="gray.700" my={6} />

      <Flex justify="space-between" fontSize="xs" color="gray.400">
        <Text>
            © {new Date().getFullYear()} Global Access Platform. All rights reserved.
        </Text>
        <Link href="#" textDecoration="underline" color="gray.200" _hover={{ color: "white" }}>
          Licensed under the MIT License
        </Link>
      </Flex>
    </Box>
  );
};

export default Footer;
