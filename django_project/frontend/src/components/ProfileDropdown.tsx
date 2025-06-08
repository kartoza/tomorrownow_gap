import React, { useState, useRef, useEffect } from 'react';
import {
  Avatar,
  AvatarFallback,
  Box,
  Button,
  Text,
  VStack,
  HStack,
  Separator,
  Collapsible,
  useDisclosure
} from '@chakra-ui/react';
// import { ChevronDownIcon, LogOutIcon, UserIcon, MailIcon } from 'lucide-react';
import { FiChevronDown, FiLogOut } from "react-icons/fi";
import { User } from '@/types'; // Adjust the import path as necessary

interface ProfileDropdownProps {
  user: User;
  size?: 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl';
  colorScheme?: string;
  onLogout?: () => void;
  onProfileClick?: () => void;
  ml?: number | string; // Optional margin-left prop
}

const ProfileDropdown: React.FC<ProfileDropdownProps> = ({
  user,
  size = 'md',
  colorScheme = 'green',
  onLogout,
  onProfileClick,
  ml = 0 // Default margin-left to 0
}) => {
  const { open, setOpen, onToggle } = useDisclosure();
  const dropdownRef = useRef<HTMLDivElement>(null);

  const fullName = `${user.first_name} ${user.last_name}`.trim();

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setOpen(false)
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  const handleLogout = () => {
    setOpen(false);
    if (onLogout) {
      onLogout();
    } else {
      // Default logout action
      console.log('Logging out...');
      // You might want to clear localStorage, redirect, etc.
    }
  };

  return (
    <Box position="relative" ref={dropdownRef} display={{ base: 'none', md: 'flex' }}>
      <Collapsible.Root open={open} onOpenChange={onToggle}>
        <Collapsible.Trigger>
          <Box
            padding={2}
            height="auto"
            borderRadius="full"
            minWidth={'unset'}
            ml={ml}
            _hover={{ bg: 'gray.100' }}
            _active={{ bg: 'gray.200' }}
            data-state={open ? 'open' : 'closed'}
          >
            <HStack gap={2}>
              <Avatar.Root size={size} colorScheme={colorScheme}>
                <Avatar.Fallback name={fullName} />
              </Avatar.Root>
              <FiChevronDown 
                size={16} 
                style={{ 
                  transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
                  transition: 'transform 0.2s ease'
                }}
              />
            </HStack>
          </Box>
        </Collapsible.Trigger>

        <Collapsible.Content>
          <Box
            position="absolute"
            top="100%"
            right={0}
            minWidth="280px"
            bg="white"
            borderRadius="lg"
            boxShadow="lg"
            border="1px solid"
            borderColor="gray.200"
            padding={3}
            zIndex={1000}
            marginTop={2}
          >
            {/* User Info Section */}
            <Box padding={3} borderRadius="md" bg="gray.50">
              <HStack gap={3} marginBottom={2}>
                <Avatar.Root size="sm" colorScheme={colorScheme}>
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
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};

export default ProfileDropdown;