"use client";

import { Avatar, useTheme } from "@mui/material";
import type { SxProps, Theme } from "@mui/material";

import { avatarPalette, initialsOf } from "@/lib/utils/avatar";

interface UserAvatarProps {
  /** Seeds the colour — the account id, so one person is one colour everywhere. */
  id: string;
  name: string;
  src?: string | null;
  size?: number;
  sx?: SxProps<Theme>;
}

/** A person's avatar at any small size: their picture when they have one,
 * otherwise their initials on a colour that is theirs alone. */
const UserAvatar = ({ id, name, src, size = 22, sx }: UserAvatarProps) => {
  const theme = useTheme();
  const palette = avatarPalette(id, theme);

  return (
    <Avatar
      src={src || undefined}
      alt={name}
      sx={{
        width: size,
        height: size,
        flexShrink: 0,
        fontSize: Math.round(size * 0.43),
        fontWeight: 700,
        letterSpacing: 0,
        bgcolor: palette.bgcolor,
        color: palette.color,
        ...sx,
      }}>
      {initialsOf(name)}
    </Avatar>
  );
};

export default UserAvatar;
