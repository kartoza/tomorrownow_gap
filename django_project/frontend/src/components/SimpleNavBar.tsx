import React from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Button, Container, Flex, HStack, Spacer, Text, useDisclosure, IconButton, Collapsible, VStack, Avatar, Separator } from '@chakra-ui/react';
import { RxHamburgerMenu } from "react-icons/rx";
import { RiCloseFill } from "react-icons/ri";
import { FiLogOut, FiKey } from "react-icons/fi";
import { toaster } from '@/components/ui/toaster';

import { RootState, AppDispatch } from '@/app/store';
import ProfileDropdown from './ProfileDropdown';
import {
    logoutUser
} from '@/features/auth/authSlice';
import { logoutEvent } from '@/utils/analytics';


const SimpleNavBar: React.FC = () => {
    const { user, isAuthenticated } = useSelector((s: RootState) => s.auth);
    const { pages, isAdmin } = useSelector((s: RootState) => s.auth);
    const { open, onToggle } = useDisclosure();
    const dispatch = useDispatch<AppDispatch>();
    const fullName = user ? `${user.first_name} ${user.last_name}`.trim() : null;

    const navigate = (path: string) => {
        window.location.href = path; // Use window.location.href for navigation
    }

    const handleLogout = async () => {
        logoutEvent();
        const resultAction = await dispatch(logoutUser());
        if (logoutUser.fulfilled.match(resultAction)) {
            toaster.create({
                title: "Logout Successful",
                description: "You have successfully logged out.",
                type: "success"
            });
            // logout successful, redirect to signin
            navigate('/signin');
        }
    }

    return (
        <Box as="nav" bg="white" boxShadow="sm" position="sticky" top={0} zIndex={1000} w="full">
            <Container px={{ base: 4, md: 6 }} w="full">
                <Flex h={16} alignItems="center">
                    <HStack gap={2} alignItems="center" onClick={() => window.location.href = '/'} _hover={{ cursor: 'pointer' }}>
                        <Box w={'23px'} h={'25px'} bgSize={"contain"} bgPos="center" bgImage="url('/static/images/gap.png')" />
                        <Text fontWeight="extrabold">Global Access Platform</Text>
                    </HStack>
                    <Spacer />
                    {/* Login Desktop Navigation */}
                    { !isAuthenticated && <Button visual="solid" size="sm" ml={4} display={{ base: 'none', lg: 'flex' }}
                        onClick={() => navigate('/signin?next=/api/v1/docs/')} // Redirect to login page
                    >
                        Log In
                    </Button>}
                    {/* User Profile Dropdown */}
                    {isAuthenticated && <ProfileDropdown ml={4} user={user} onLogout={handleLogout} navigateFn={navigate} />}
                    {/* Mobile Hamburger Button */}
                    <IconButton
                        display={{ base: 'flex', lg: 'none' }}
                        onClick={onToggle}
                        variant="ghost"
                        aria-label="Toggle Navigation"
                        _hover={{ bg: 'gray.100' }}
                        minW={'unset'}
                    >
                        {open ? <RiCloseFill /> : <RxHamburgerMenu />}
                    </IconButton>
                </Flex>

                {/* Mobile Menu */}
                <Collapsible.Root open={open} onOpenChange={onToggle}>
                    <Collapsible.Content>
                        <Box
                            position="absolute"
                            top="100%"
                            right={0}
                            bg="white"
                            boxShadow="lg"
                            borderRadius="md"
                            border="1px"
                            borderColor="gray.200"
                            zIndex={999}
                            display={{ lg: 'none' }}
                            mx={4}
                            mt={2}
                        >
                            <VStack gap={0} alignItems="stretch" p={4}>
                                {/* Mobile Login Button */}
                                {!isAuthenticated && <Button visual="solid" size="md" w="full" mt={2} onClick={() => {
                                    onToggle(); // Close menu after clicking
                                    navigate('/signin?next=/api/v1/docs/'); // Redirect to login page
                                }
                                }>
                                    Log In
                                </Button>
                                }
                                {isAuthenticated && (
                                    <Box>
                                        <Separator marginY={3} />
                                        <Box padding={3} borderRadius="md" bg="gray.50">
                                            <HStack gap={3} marginBottom={2}>
                                            <Avatar.Root size="sm" colorScheme={"green"}>
                                                <Avatar.Fallback name={fullName} />
                                            </Avatar.Root>
                                            <VStack gap={0} alignItems="flex-start" flex={1}>
                                                <Text fontWeight="semibold" lineHeight="1.2">
                                                {fullName}
                                                </Text>
                                                <Text fontSize="sm" color="gray.600" lineHeight="1.2">
                                                {user.email}
                                                </Text>
                                            </VStack>
                                            </HStack>
                                        </Box>

                                        <Separator marginY={1} />

                                        {/* DCAS CSV (KALRO only) */}
                                        {(isAdmin || pages.includes('dcas_csv')) && (
                                            <Button
                                            variant="ghost"
                                            size="sm"
                                            width="100%"
                                            justifyContent="flex-start"
                                            padding={3}
                                            height="auto"
                                            borderRadius="md"
                                            onClick={() => {
                                                onToggle();
                                                navigate('/dcas-csv');
                                            }}
                                            >
                                            <HStack gap={3} width="100%">
                                                <Text fontSize="sm" fontWeight="semibold">
                                                DCAS CSV
                                                </Text>
                                            </HStack>
                                            </Button>
                                        )}

                                        <Button
                                            variant="ghost"
                                            size="sm"
                                            width="100%"
                                            justifyContent="flex-start"
                                            padding={3}
                                            height="auto"
                                            borderRadius="md"
                                            onClick={() => {
                                            onToggle();
                                            navigate('/api-keys');       // go to the page
                                            }}
                                        >
                                            <HStack gap={3} w="full">
                                            <FiKey size={6} />
                                            <Text fontSize="sm" fontWeight="semibold">
                                                API&nbsp;Keys
                                            </Text>
                                            </HStack>
                                        </Button>
                            
                                        {/* Logout Menu Item */}
                                        <Button
                                            variant="ghost"
                                            width="100%"
                                            justifyContent="flex-start"
                                            padding={3}
                                            height="auto"
                                            borderRadius="md"
                                            _hover={{ bg: 'red.50', color: 'red.600' }}
                                            color="red.500"
                                            onClick={handleLogout}
                                        >
                                            <HStack gap={3} width="100%">
                                                <FiLogOut size={16} />
                                                <Text>Logout</Text>
                                            </HStack>
                                        </Button>
                                    </Box>
                                )}
                            </VStack>
                        </Box>
                    </Collapsible.Content>
                </Collapsible.Root>
            </Container>
        </Box>
    );
};

export default SimpleNavBar;