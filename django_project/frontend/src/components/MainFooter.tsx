import React from 'react';
import {
    Box,
    Container,
    Grid,
    GridItem,
    Heading,
    VStack,
    Link,
    Text,
    Separator as Divider,
    Flex
} from '@chakra-ui/react';
import { openInNewTab } from '@/utils/url';
import { APIDocsURL } from '@/utils/constants';
import { useNavigateWithEvent } from '@/hooks/useNavigateWithEvent';

const MainFooter: React.FC = () => {
    const navigate = useNavigateWithEvent();
    return (
        <Box bg="brand.600" color="white" py={12}>
            <Container px={{ base: 6, md: 0 }} w="full" >
                <Grid templateColumns={{ base: '1fr', md: 'repeat(2, 1fr)' }} gap={8}>
                    <GridItem justifyContent={{ base: 'start', md: 'center' }} display="flex">
                            <VStack gap={2} align="start">
                                <Heading as="h4" size="xl" fontWeight={"bold"}>Resources</Heading>
                                <VStack gap={2} align="start">
                                    <Link color="white" _hover={{ textDecoration: 'underline' }} onClick={() => openInNewTab(APIDocsURL)} >API Docs</Link>
                                    <Link color="white" _hover={{ textDecoration: 'underline' }}>News</Link>
                                </VStack>
                        </VStack>
                    </GridItem>
        
                    <GridItem justifyContent={{ base: 'start', md: 'center' }} display="flex">
                        <VStack gap={2} align="start">
                            <Heading as="h4" size="xl" fontWeight={"bold"}>Contact Us</Heading>
                            <VStack gap={2} align="start">
                                <Link color="text.secondary" fontWeight={"bold"} onClick={() => navigate('/signup', 'footer_sign_up')}>Join Our Waitlist</Link>
                            </VStack>
                        </VStack>
                    </GridItem>
                </Grid>
    
                <Divider my={8} borderColor="white" />
                
                <Flex justify="center" align="center" flexDirection={{ base: 'column', md: 'row' }} gap={4}>
                    <Text fontSize="sm" color="text.muted">
                        © 2025 Global Access Platform. All rights reserved.
                    </Text>
                </Flex>
            </Container>
        </Box>
    );
};

export default MainFooter;