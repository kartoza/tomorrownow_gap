import React from "react";
import {
    Box,
    Button,
    Container,
    Heading,
    Text,
    VStack,
    Flex
} from "@chakra-ui/react";
import { useNavigateWithEvent } from "@/hooks/useNavigateWithEvent";

interface HeroSectionProps {
    children?: React.ReactNode;
}

const HeroSection: React.FC<HeroSectionProps> = ({ children }) => {
    const navigate = useNavigateWithEvent();
    return (
        <Flex
            alignItems="center"
            color="white"
            position={'relative'}
            bgImage="url('/static/images/auth_bg.webp')"
            bgSize="cover"
            bgPos="center"
            bgAttachment="fixed"
            w="full"
            flex={1}
            direction={{ base: 'column', md: 'row' }}
            gap={{ base: 4, md: 0 }}
        >
            {/* Left Section */}
            <Flex flex={{ base: '1 1 50%', xl: '1 1 64.51%'  }} display={{ base: 'none', md: 'flex' }} alignItems="center" position="relative" backdropFilter={'blur(2px)'} bgColor="rgba(0, 213, 142, 0.05)">
                <Container px={{ base: 4, xl: 8 }} maxW={{ base: "70%", md: "85%" }}>
                    <VStack gap={6} w="full" alignItems={{ base: "center", md: "start" }} textAlign={{ base: "center", md: "start" }}>
                        <Heading as="h1" size={{ base: "md", xl: "lg" }} variant={"mainTitle"}>
                            Unlocking Weather Resilience for Smallholder Farmers
                        </Heading>
                        <Text fontSize="2xl" fontWeight="extrabold" mt={{ base: 12, md: 5 }}>
                            Validated and localized agromet products, trusted by leading farmer-facing organisations.
                        </Text>
                        <Button visual="outline" size="md" onClick={() => navigate('/signup', 'auth_hero_section_join_us')}>
                            Join Us
                        </Button>
                    </VStack>
                </Container>
            </Flex>

            {/* Right Section */}
            <Flex flex={{base: '1 1 50%', xl: '1 1 35.49%' }} w="full" h="full" alignItems="center" position="relative" backdropFilter={'blur(2px)'} bgColor="rgba(0, 213, 142, 0.55)" borderTopLeftRadius={{ base: '0', md: 'buttonSm' }} borderBottomLeftRadius={{ base: '0', md: 'buttonSm' }}>
                <Container px={{ base: 4, xl: 8 }} w="full" maxW="100%">
                    {children}
                </Container>
            </Flex>
        </Flex>
    );
};

export default HeroSection;