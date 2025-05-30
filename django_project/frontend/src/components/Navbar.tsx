import React from 'react';
import {
  Box,
  Flex,
  Button,
  Link,
  Text,
  useDisclosure,
  Spacer,
  HStack,
  Stack,
  Portal,
  Dialog,
  Menu,
  CloseButton,
} from '@chakra-ui/react';
import SignIn from '../pages/Login';

const partners = [
  'CGIAR',
  'One Acre Fund',
  'Regenorganics',
  'Rhiza Research',
  'Salient',
  'TAHMO',
  'Tomorrow.io',
  'Tomorrownow.org',
];

const Navbar: React.FC = () => {
  const {
    open: isLoginOpen,
    onOpen: onLoginOpen,
    onClose: onLoginClose,
  } = useDisclosure();

  const DesktopLinks = () => (
    <HStack gap={20} fontWeight="bold" fontSize="sm">
      <Link href="#products" _hover={{ textDecoration: 'underline', color: 'green.500' }}>Products</Link>
      <Link href="#data-access" _hover={{ textDecoration: 'underline', color: 'green.500' }}>Data Access</Link>
      <Menu.Root>
        <Menu.Trigger>
          <Link
            fontWeight="bold"
            fontSize="sm"
            _hover={{ color: 'green.500', textDecoration: 'underline' }}
            cursor="pointer"
          >
            Our Partners
          </Link>
        </Menu.Trigger>
        <Portal>
          <Menu.Positioner>
            <Menu.Content>
              {partners.map((name) => (
                <Menu.Item>
                  <Link
                    href={`#partner-${name.toLowerCase().replace(/\s+/g, '-')}`}
                    _hover={{ textDecoration: 'none', bg: 'gray.100' }}
                  >
                    {name}
                  </Link>
                </Menu.Item>
              ))}
            </Menu.Content>
          </Menu.Positioner>
        </Portal>
      </Menu.Root>
      <Link href="#about" _hover={{ textDecoration: 'underline', color: 'green.500' }}>About Us</Link>
    </HStack>
  );

  const MobileLinks = ({ onClose }: { onClose: () => void }) => (
    <Stack gap={4} mt={4}>
      <Link href="#products" onClick={onClose}>Products</Link>
      <Link href="#data-access" onClick={onClose}>Data Access</Link>
      <Box>
        <Text fontWeight="semibold" mb={1}>Our Partners</Text>
        <Stack pl={4}>
          {partners.map((name) => (
            <Link
              key={name}
              href={`#partner-${name.toLowerCase().replace(/\s+/g, '-')}`}
              onClick={onClose}
            >
              {name}
            </Link>
          ))}
        </Stack>
      </Box>
      <Link href="#about" onClick={onClose}>About Us</Link>
    </Stack>
  );

  return (
    <Box bg="white" px={{ base: 4, md: 10 }} shadow="sm">
      <Flex h={16} align="center">
        <HStack gap={4}>
          <Link
            href="/"
            fontWeight="bold"
            fontSize="md"
            _hover={{ textDecoration: 'underline', color: 'green.500' }}
          >
            Global Access Platform
          </Link>
        </HStack>

        <Spacer />

        <HStack gap={8}>
          <DesktopLinks />
          <Button
            onClick={onLoginOpen}
            rounded="full"
            bg="green.400"
            _hover={{ bg: 'green.500' }}
            color="black"
            px={4}
            py={2}
            fontSize="sm"
            fontWeight="bold"
            display={{ base: 'none', md: 'inline-flex' }}
          >
            Log In
          </Button>

          <Dialog.Root>
            <Portal>
              <Dialog.Backdrop />
              <Dialog.Positioner>
                <Dialog.Content>
                  <Box bg="white" p={4}>
                    <Dialog.Header>
                      <Dialog.Title>Menu</Dialog.Title>
                    </Dialog.Header>
                    <Dialog.Body>
                      <MobileLinks onClose={() => {}} />
                      <Button
                        onClick={() => {
                          onLoginOpen();
                        }}
                        rounded="full"
                        bg="green.400"
                        _hover={{ bg: 'green.500' }}
                        color="black"
                        px={4}
                        py={2}
                        fontSize="sm"
                        fontWeight="bold"
                        mt={4}
                      >
                        Log In
                      </Button>
                    </Dialog.Body>
                    <Dialog.CloseTrigger>
                      <CloseButton/>
                    </Dialog.CloseTrigger>
                  </Box>
                </Dialog.Content>
              </Dialog.Positioner>
            </Portal>
          </Dialog.Root>
        </HStack>
      </Flex>

      <Portal>
        <SignIn isOpen={isLoginOpen} onClose={onLoginClose} />
      </Portal>
    </Box>
  );
};

export default Navbar;
