import React, { useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Button, Container, Flex, HStack, Link, Spacer, Text, useDisclosure, IconButton, Collapsible, VStack, Avatar, Separator } from '@chakra-ui/react';
import { RxHamburgerMenu } from "react-icons/rx";
import { RiCloseFill } from "react-icons/ri";
import { useLocation, useNavigate } from 'react-router-dom';
import { FiLogOut } from "react-icons/fi";
import { toaster } from '@/components/ui/toaster';

import { useScrollContext } from '@/context/ScrollContext';
import { handleSmoothScroll } from '@/utils/scroll';
import { openInNewTab } from '@/utils/url';
import { APIDocsURL } from '@/utils/constants';
import { RootState, AppDispatch } from '@/app/store';
import ProfileDropdown from './ProfileDropdown';
import {
    logoutUser,
    fetchUserInfo,
    fetchPermittedPages
} from '@/features/auth/authSlice';
import { useNavigateWithEvent } from '@/hooks/useNavigateWithEvent';
import { logoutEvent } from '@/utils/analytics';


interface NavItem {
  label: string;
  href: string;
}

const Navigation: React.FC = () => {
    const { loading, user, isAuthenticated, hasInitialized } = useSelector((s: RootState) => s.auth);
    const { open, onToggle } = useDisclosure();
    const { activeSection } = useScrollContext();
    const dispatch = useDispatch<AppDispatch>();
    const location = useLocation();
    const navigate = useNavigateWithEvent();
    const navItems: NavItem[] = [
        { label: 'Products', href: '#products' },
        { label: 'Data Access', href: APIDocsURL },
        { label: 'Our Partners', href: '#partners' },
        { label: 'About Us', href: '#about' }
    ];
    const fullName = user ? `${user.first_name} ${user.last_name}`.trim() : null;

    useEffect(() => {
        if (!hasInitialized && !loading) {
            dispatch(fetchUserInfo())
        }
    }, [loading, hasInitialized, dispatch]);

    useEffect(() => {
        if (isAuthenticated) {
          dispatch(fetchPermittedPages())
        }
      }, [isAuthenticated, dispatch])

    const handleMenuClick = (sectionId: string) => {
        // Check if we're on the landing page
        if (location.pathname === '/') {
            // We're on the same page, just scroll to section
            handleSmoothScroll(sectionId);
        } else {
            // We're on a different page, navigate to landing page
            navigate(`/${sectionId}`, `navbar_${sectionId.replace('#', '')}`);
        }
    };

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
            navigate('/signin', null, true);
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
                    {/* Desktop Navigation */}
                    <HStack gap={8} display={{ base: 'none', lg: 'flex' }}>
                        {navItems.map((item) => (
                        <Link
                            key={item.label}
                            href={item.href}
                            onClick={(e) => {
                                e.preventDefault();
                                if (item.href.startsWith('#')) {
                                    handleMenuClick(item.href);
                                } else if (item.href !== '') {
                                    openInNewTab(item.href);
                                }
                            }}
                            fontWeight={"extrabold"}
                            color={activeSection === item.href && item.href != '' ? "brand.500" : "text.primary"}
                            _hover={{ color: 'brand.500', textDecoration: 'none' }}
                            position="relative"
                            transition="all 0.2s ease"  
                        >
                            {item.label}
                        </Link>
                        ))}
                    </HStack>
                    {/* Login Desktop Navigation */}
                    { !isAuthenticated && <Button visual="solid" size="sm" ml={4} display={{ base: 'none', lg: 'flex' }}
                        onClick={() => navigate('/signin')} // Redirect to login page
                    >
                        Log In
                    </Button>}
                    {/* User Profile Dropdown */}
                    {isAuthenticated && <ProfileDropdown ml={4} user={user} onLogout={handleLogout} />}
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
                                {navItems.map((item) => (
                                    <Link
                                        key={item.label}
                                        href={item.href}
                                        onClick={(e) => {
                                            e.preventDefault();
                                            onToggle(); // Close menu after clicking
                                            if (item.href.startsWith('#')) {
                                                handleMenuClick(item.href);
                                            } else if (item.href !== '') {
                                                openInNewTab(item.href);
                                            }
                                        }}
                                        fontWeight={"extrabold"}
                                        color={activeSection === item.href && item.href != '' ? "brand.500" : "text.primary"}
                                        _hover={{ color: 'brand.500', textDecoration: 'none' }}
                                        fontSize="lg"
                                        py={1}
                                        w="full"
                                        textAlign="left"
                                    >
                                        {item.label}
                                    </Link>
                                ))}
                                
                                {/* Mobile Login Button */}
                                {!isAuthenticated && <Button visual="solid" size="md" w="full" mt={2} onClick={() => {
                                    onToggle(); // Close menu after clicking
                                    navigate('/signin'); // Redirect to login page
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

export default Navigation;