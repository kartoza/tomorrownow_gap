import React from 'react';
import { Box, Button, Container, Flex, HStack, Link, Spacer, Text, useDisclosure, IconButton, Collapsible, VStack } from '@chakra-ui/react';
import { RxHamburgerMenu } from "react-icons/rx";
import { RiCloseFill } from "react-icons/ri";
import { useLocation, useNavigate } from 'react-router-dom';

import { useScrollContext } from '@/context/ScrollContext';
import { handleSmoothScroll } from '@/utils/scroll';
import { openInNewTab } from '@/utils/url';
import { APIDocsURL } from '@/utils/constants';

interface NavItem {
  label: string;
  href: string;
}

const Navigation: React.FC = () => {
    const { open, onToggle } = useDisclosure();
    const { activeSection } = useScrollContext();
    const location = useLocation();
    const navigate = useNavigate();
    const navItems: NavItem[] = [
        { label: 'Products', href: '#hub' },
        { label: 'Data Access', href: APIDocsURL },
        { label: 'Our Partners', href: '#partners' },
        { label: 'About Us', href: '#about' }
    ];

    const handleMenuClick = (sectionId: string) => {
        // Check if we're on the landing page
        if (location.pathname === '/') {
            // We're on the same page, just scroll to section
            handleSmoothScroll(sectionId);
        } else {
            // We're on a different page, navigate to landing page
            navigate(`/${sectionId}`);
        }
    };

    return (
        <Box as="nav" bg="white" boxShadow="sm" position="sticky" top={0} zIndex={1000} w="full">
            <Container px={{ base: 4, md: 6 }} w="full">
                <Flex h={16} alignItems="center">
                    <HStack gap={2} alignItems="center" onClick={() => window.location.href = '/'} _hover={{ cursor: 'pointer' }}>
                        <Box w={'23px'} h={'25px'} bgSize={"contain"} bgPos="center" bgImage="url('/static/images/gap.png')" />
                        <Text fontWeight="bold">Global Access Platform</Text>
                    </HStack>
                    <Spacer />
                    {/* Desktop Navigation */}
                    <HStack gap={8} display={{ base: 'none', md: 'flex' }}>
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
                            fontWeight={activeSection === item.href && item.href != '' ? "bold" : "medium"}
                            color={activeSection === item.href && item.href != '' ? "brand.500" : "text.primary"}
                            _hover={{ color: 'brand.500' }}
                            position="relative"
                            transition="all 0.2s ease"
                            _after={activeSection === item.href && item.href != '' ? {
                            content: '""',
                            position: 'absolute',
                            bottom: '-4px',
                            left: '0',
                            right: '0',
                            height: '2px',
                            bg: 'brand.500',
                            borderRadius: 'full',
                            } : {}}
                        >
                            {item.label}
                        </Link>
                        ))}
                    </HStack>
                    {/* Login Desktop Navigation */}
                    <Button visual="solid" size="sm" ml={4} display={{ base: 'none', md: 'flex' }}
                        onClick={() => navigate('/signin')} // Redirect to login page
                    >
                        Log In
                    </Button>
                    {/* Mobile Hamburger Button */}
                    <IconButton
                        display={{ base: 'flex', md: 'none' }}
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
                            display={{ md: 'none' }}
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
                                        fontWeight={activeSection === item.href && item.href != '' ? "bold" : "medium"}
                                        color={activeSection === item.href && item.href != '' ? "brand.500" : "text.primary"}
                                        _hover={{ color: 'brand.500' }}
                                        fontSize="lg"
                                        py={1}
                                        w="full"
                                        textAlign="left"
                                    >
                                        {item.label}
                                    </Link>
                                ))}
                                
                                {/* Mobile Login Button */}
                                <Button visual="solid" size="md" w="full" mt={2} onClick={() => {
                                    onToggle(); // Close menu after clicking
                                    navigate('/signin'); // Redirect to login page
                                }
                                }>
                                    Log In
                                </Button>
                            </VStack>
                        </Box>
                    </Collapsible.Content>
                </Collapsible.Root>
            </Container>
        </Box>
    );
};

export default Navigation;