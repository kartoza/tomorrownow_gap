export const isPasswordStrong = (password: string): string | null => {
    const minLength = 8;
    const hasUppercase = /[A-Z]/.test(password);
    const hasDigit = /[0-9]/.test(password);
    const hasSpecial = /[!@#$%^&*(),.?":{}|<>]/.test(password);
  
    if (password.length < minLength) {
      return 'Password must be at least 8 characters long.';
    }
    if (!hasUppercase) {
      return 'Password must contain at least one uppercase letter.';
    }
    if (!hasDigit) {
      return 'Password must contain at least one number.';
    }
    if (!hasSpecial) {
      return 'Password must contain at least one special character.';
    }
  
    return null; // Valid password
  };
  