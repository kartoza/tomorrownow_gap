import React from "react";
import {
    Box,
    Button,
    Container,
    Heading,
    Text,
    VStack
} from "@chakra-ui/react";
import { useNavigate } from "react-router-dom";


const HeroSection: React.FC = () => {
    const navigate = useNavigate();
    return (
        <Box
            h="100vh"
            display="flex"
            alignItems="center"
            color="white"
            position={'relative'}
            bgImage="url('/static/images/home.webp')"
            bgSize="cover"
            bgPos="center"
            bgAttachment="fixed"
            w="full"
        >
            <Box h='100vh' w="full" display="flex" alignItems="center" position="relative" backdropFilter={'blur(2px)'} bgColor="rgba(0, 213, 142, 0.05)">
                <Container px={{ base: 9, md: 16 }} w="full">
                    <VStack gap={6} w="full" alignItems={{ base: "center", md: "start" }} textAlign={{ base: "center", md: "start" }}>
                        <Heading as="h1" size={{ base: "md", md: "lg" }} variant={"mainTitle"}>
                            Global Access Platform
                        </Heading>
                        <Text fontSize={{ base: "3xl", md: "subGiant" }} fontWeight="extrabold" lineHeight="moderate">
                            For Agro-Met Intelligence
                        </Text>
                        <Text fontSize="2xl" fontWeight="extrabold" mt={{ base: 12, md: 5 }}>
                            Unlocking Weather Resilience for Smallholder Farming
                        </Text>
                        <Button visual="solid" size="md" onClick={() => navigate('/signup')}>
                            Join Us
                        </Button>
                    </VStack>
                </Container>
            </Box>
        </Box>
    );
};

export default HeroSection;