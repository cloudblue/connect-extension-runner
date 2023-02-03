import os
import sys

from pyfiglet import (
    Figlet,
    FigletFont,
    parse_color,
)


LEVEL_TO_FONT_COLOR = {
    'INFO': ('ansi_regular', 'BLUE'),
    'WARNING': ('bloody', 'YELLOW'),
    'ERROR': ('bloody', 'RED'),
}


def install_custom_fonts():
    fonts = FigletFont.getFonts()
    for font in ('ansi_regular', 'bloody'):
        if font not in fonts:
            FigletFont.installFonts(
                os.path.join(
                    os.path.abspath(os.path.dirname(__file__)),
                    f'{font}.flf',
                ),
            )


def print_banner(level='INFO'):
    font, color = LEVEL_TO_FONT_COLOR[level]

    figlet = Figlet(font=font)
    r = figlet.renderText('Connect Extension Runner')

    ansiColors = parse_color(color.upper())
    if ansiColors:
        sys.stdout.write(ansiColors)

    sys.stdout.write(r)
    sys.stdout.write('\n')
    sys.stdout.write(parse_color('WHITE'))


install_custom_fonts()
