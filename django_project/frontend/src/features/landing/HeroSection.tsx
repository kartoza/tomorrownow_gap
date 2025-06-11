import React from "react";
import {
    Box,
    Button,
    Container,
    Heading,
    Text,
    VStack
} from "@chakra-ui/react";
import { useNavigateWithEvent } from "@/hooks/useNavigateWithEvent";
import { HeroBgImageUrl } from "./Images";

const HeroSection: React.FC = () => {
    const navigate = useNavigateWithEvent();
    return (
        <Box
            id="hero"
            h="100vh"
            display="flex"
            alignItems="center"
            color="white"
            position={'relative'}
            bgImage={`url('${HeroBgImageUrl()}')`}
            bgSize="cover"
            bgPos="center"
            // disable fixed bg on mobile because ios bug with fixed backgrounds
            bgAttachment={{ base: "unset", lg: "fixed" }}
            w="full"
        >
            <Box h='100vh' w="full" display="flex" alignItems="center" position="relative" backdropFilter={'blur(2px)'} bgColor="rgba(0, 213, 142, 0.05)">
                <Container px={{ base: 9, md: 6 }} w="full">
                    <VStack gap={6} w="full" alignItems={{ base: "center", md: "start" }} textAlign={{ base: "center", md: "start" }}>
                        <Heading as="h1" size={{ base: "md", md: "lg" }} variant={"mainTitle"}>
                            Global Access Platform
                        </Heading>
                        <Text fontSize={{ base: "3xl", md: "subGiant" }} fontWeight="extrabold" lineHeight="moderate">
                            For AgroMet Intelligence
                        </Text>
                        <Text fontSize="2xl" fontWeight="extrabold" mt={{ base: 12, md: 5 }}>
                            Unlocking Weather Resilience for Smallholder Farming
                        </Text>
                        <Button visual="solid" size="md" onClick={() => navigate('/signup', 'hero_section_join_us')}>
                            Join Us
                        </Button>
                    </VStack>
                </Container>
            </Box>
        </Box>
    );
};

export default HeroSection;